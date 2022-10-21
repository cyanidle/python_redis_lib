from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass
import logging
from typing import Any, Callable, List, Tuple, Union
from typing_extensions import Self
import aiomysql
from pymysql.err import OperationalError
from .async_decorators import *
from .supervisor import WorkerBase
from .settings import SerializableDataclass, SqlClientSettings

log = logging.getLogger("sql-client")

async def sql_handler(coro, *args, **kwargs):
    self: SqlClient = args[0]
    while True:
        try:
            if self.connected:                   
                return await coro(*args, **kwargs)  
            else:
                if not self._was_run:
                    log.warn(f"Using Sql without calling run() method!")
                await asyncio.sleep(1)
        except OperationalError as e:
            log.error(e)
            return None
        except ConnectionRefusedError as e:
            log.warn(f"Connection refused! Full: {e}")
            self.connected = False
            await asyncio.sleep(10)
            self.ioloop.create_task(self._on_creation())
        except TimeoutError as e:
            log.warn(f"Timeout happened! Full: {e}")
            self.connected = False
            self.ioloop.create_task(self._on_creation())
        except:
            raise
    
class SqlClient(WorkerBase):
    def __init__(self, settings: SqlClientSettings,*, ioloop: asyncio.AbstractEventLoop) -> None:
        self.ioloop = ioloop
        self._pool = None
        self.queries = asyncio.Queue()
        self.settings = settings
        self._connected = False
        self._was_run = False

    @property
    def connected(self) -> bool:
        return self._connected

    @connected.setter
    def connected(self,value):
        if self._connected!=value:
            log.info(f"Mysql ({self.settings.server.name}) is now connected --> {value}")
        self._connected = value


    def run(self):
        self._was_run = True
        self.ioloop.create_task(self._on_creation())

    def TABLE(self, name:str):
        # TODO: Check if table exists
        return _SqlTable(self, name)

    @async_oneshot
    @async_handle_exceptions(sql_handler)
    async def execute(self, query: str) -> Tuple[Any, ...]:
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                responce = await cur.fetchall()
                await conn.commit()
                if responce:
                    if len(responce) > 1:
                        log.warn(f"Multiple responces for single execute() call!")
                    return responce[0]
                else:
                    return tuple()

    @async_oneshot
    @async_handle_exceptions(sql_handler)
    async def execute_many(self, queries: Union[List[str], str]) -> Tuple[Tuple[Any, ...]]:
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                if isinstance(queries, list):
                    for query in queries:
                        await cur.execute(query)
                elif isinstance(queries, str):
                    await cur.execute(queries)
                else:
                    raise TypeError("Queries must be 'str' or 'list[str]'")
                responce = await cur.fetchall()
                await conn.commit()
                return responce

    @async_oneshot
    async def _on_creation(self):
        try:
            self._pool = await aiomysql.create_pool(host=self.settings.host, port=self.settings.port,
                                            user=self.settings.user, password=self.settings.password,
                                            db=self.settings.db, loop=self.ioloop)
            self.connected = True
        except OperationalError as e:
            log.error(e)
            self.connected = False
            await asyncio.sleep(10)
            self.ioloop.create_task(self._on_creation())
        except ConnectionRefusedError as e:
            log.warn(f"Connection refused! Full: {e}")
            self.connected = False
            await asyncio.sleep(10)
            self.ioloop.create_task(self._on_creation())
        except TimeoutError as e:
            log.warn(f"Timeout happened! Full: {e}")
            self.connected = False
            self.ioloop.create_task(self._on_creation())

    def handleTerm(self):
        self._pool.close()


@dataclass
class _SqlTable:
    _parent: SqlClient
    name: str
    def SELECT(self, *keys: Union[Tuple[str], str]):
        if not keys:
            raise SyntaxError("Empty SELECT")
        else:
            return _SelectAction(self, tuple(keys))
    def INSERT(self, *keys: Union[Tuple[str], str]):
        if not keys:
            raise SyntaxError("Empty INSERT")
        else:
            return _InsertAction(self, tuple(keys))

class _SqlAction(ABC):

    @abstractmethod
    async def exec(self):
        raise NotImplementedError

    def __await__(self):
        return self.exec().__await__()


@dataclass(slots=True)
class _InsertAction(_SqlAction):
    _parent: _SqlTable
    _keys: Tuple[str]
    _has_values: bool = False
    _values: Tuple[str] = ()

    def _prepare(self) -> str:
        if len(self._keys) == 1:
            self._keys = self._keys[0]
            if not self._has_values:
                raise SyntaxError("No VALUES passed!")
            elif len(self._values) > 1:
                raise SyntaxError("More VALUES, than KEYS")
            self._values = self._values[0]
        if self._values:
            fields_string = f"({', '.join(self._keys)})"
            values_string = str('(')
            for string_value in self._values:
                if string_value == "NULL":
                    values_string += "NULL"
                else:
                    values_string += f"'{string_value}'"
                values_string += ', '
            values_string = values_string[:-2] + ')'
            return f"INSERT INTO {self._parent.name} {fields_string} VALUES {values_string}"
        else:
            raise SyntaxError("No VALUES in INSERT!")

    async def exec(self) -> Tuple[Any, ...]:
        return await self._parent._parent.execute(self._prepare())
        # TODO: commit changes

    def VALUES(self, *values: Tuple[str]):
        if self._has_values:
            raise SyntaxError("VALUES already called!")
        self._has_values = True
        self._values: Tuple[str] = values
        return self

@dataclass(slots=True)
class _SelectAction(_SqlAction):
    _parent: _SqlTable
    _keys: Tuple[str]
    _where: bool = False
    _where_args: str = ""
    _order_by: str = ""
    _limit: str = ""

    def _prepare(self) -> str:
        if len(self._keys) == 1:
            self._keys = self._keys[0]
        return f"SELECT {self._keys} FROM {self._parent.name}{self._format_where}{self._format_order_by}{self._format_limit}"
        
    async def exec(self) -> Tuple[Any, ...]:
        # TODO: parse results in meaningful way
        query = self._prepare()
        result = await self._parent._parent.execute(query)
        return result

    async def exec_many(self) -> Tuple[Tuple[Any, ...]]:
        # TODO: parse results in meaningful way
        query = self._prepare()
        result = await self._parent._parent.execute_many(query)
        return result

    @property
    def _format_order_by(self):
        if self._order_by:
            return f" ORDER BY {self._order_by}"
        else:
            return ""

    @property
    def _format_where(self):
        if self._where:
            return f" WHERE {self._where_args}"
        else:
            return ""

    @property
    def _format_limit(self):
        if self._limit:
            return f" LIMIT {self._limit}"
        else:
            return ""

    def IN(self, condtition: Union[Self, str]):
        if not self._where:
            raise SyntaxError(f"IN used before WHERE!")
        if isinstance(condtition, _SelectAction):
            self._where_args += f" IN ({condtition._prepare()})"
        elif isinstance(condtition, str):
            self._where_args += f" IN ({condtition})"
        else:
            raise SyntaxError(f"Unsupported operand for IN: '{condtition}'")
        return self

    def LIMIT(self, condition:Union[str, int]):
        if not isinstance(condition, (int, str)):
            raise SyntaxError(f"Unsupported condition for LIMIT operator")
        if not condition:
            raise SyntaxError("Empty conditions!")
        self._limit = str(condition)
        return self

    def WHERE(self, conditions:str) -> Self:
        if self._where:
            raise SyntaxError("Cannot use WHERE twice!")
        if not conditions:
            raise SyntaxError("Empty conditions!")
        self._where = True
        self._where_args = str(conditions)
        return self

    def AND(self, conditions:str) -> Self:
        if not self._where:
            raise SyntaxError("AND without WHERE!")
        self._where_args += f" and {conditions}"
        return self

    def OR(self, conditions:str) -> Self:
        if not self._where:
            raise SyntaxError("OR without WHERE!")
        self._where_args += f" or {conditions}"
        return self

    def ORDER_BY(self, conditions:str) -> Self:
        if self._order_by:
            raise SyntaxError("Second ORDER BY call!")
        self._order_by = str(conditions)
        return self