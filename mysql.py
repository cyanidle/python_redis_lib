from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, List, Tuple, Union
from typing_extensions import Self
import aiomysql
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
            log.warn(f"Mysql ({self.settings.server.name}) is now connected --> {value}")
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
                responce = await cur.fetchone()
                return responce
        # TODO: catch relevant errors

    @async_oneshot
    async def _on_creation(self):
        try:
            self._pool = await aiomysql.create_pool(host=self.settings.host, port=self.settings.port,
                                            user=self.settings.user, password=self.settings.password,
                                            db=self.settings.db, loop=self.ioloop)
            self.connected = True
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
            raise _InsertAction("Empty INSERT")
        else:
            return _InsertAction(self, tuple(keys))

# INSERT INTO polderdata.forecasting (station_id, forecast_param_id, value) VALUES ( {station_id}, 5, {mesh_path_diff} );

class _SqlAction(ABC):

    @abstractmethod
    async def exec(self):
        raise NotImplementedError

    def __await__(self):
        return self.exec().__await__()


@dataclass
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
            return f"INSERT INTO {self._parent.name} {self._keys} VALUES {self._values}"
        else:
            raise SyntaxError("No VALUES in INSERT!")

    async def exec(self) -> Tuple[Any, ...]:
        return await self._parent._parent.execute(self._prepare())
        # TODO: commit changes

    def VALUES(self, *values: Tuple[str]):
        if self._has_values:
            raise SyntaxError("VALUES already called!")
        self._has_values = True
        self._values = values
        return self

@dataclass
class _SelectAction(_SqlAction):
    _parent: _SqlTable
    _keys: Tuple[str]
    _where: bool = False
    _where_args: str = ""
    _order_by: str = ""

    def _prepare(self) -> str:
        if len(self._keys) == 1:
            self._keys = self._keys[0]
        if self._order_by:
            self._order_by = f"ORDER BY {self._order_by}"
        return f"SELECT {self._keys} FROM {self._parent.name}{self._format_where}{self._format_order_by}"
        
    async def exec(self) -> Tuple[Any, ...]:
        # TODO: parse results in meaningful way
        result = await self._parent._parent.execute(self._prepare())
        if result is None:
            return None
        if type(self._keys) is str:
            return result[0]
        else:
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