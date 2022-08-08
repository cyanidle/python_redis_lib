# -*- coding: utf-8 -*-
from asyncio import coroutines
from dataclasses import dataclass, field
import asyncio
from sys import call_tracing
import aioredis
import logging
import logging.handlers
import traceback
import functools
from typing import Dict as Dict_t, List, Union
from typing import Any

from .async_decorators import LoopContinue, LoopSleep, LoopReturn, async_handle_exceptions, async_oneshot, async_repeating_task
from .settings import RedisSettings, RedisEntries

log = logging.getLogger(__name__)

BLOCK_DELAY = 0
ENABLE_LOCALSTORAGE = False

async def redis_hadler(coro, *args, **kwargs):
    try:
        self = args[0]
        if self.connected:
            return await coro(*args, **kwargs)
        else:
            await asyncio.sleep(1)
    except aioredis.exceptions.ConnectionError:
        self.connected = False
    except aioredis.exceptions.ResponseError:
        self.connected = False
        raise LoopSleep(1)

class RedisClient():
    def __init__(self, settings: RedisSettings, *, ioloop:asyncio.AbstractEventLoop, write_cb = None):
        self.host = settings.host
        self.port = settings.port
        self.max_pub_length = settings.max_pub_length
        self.max_sub_length = settings.max_sub_length
        self.ioloop = ioloop
        self.commands_stream_key = settings.commands_stream
        self.output_stream_key = settings.output_stream
        self._connected = False
        url = f'redis://{self.host}:{self.port}'
        log.info(f"Using url: {url}. Please, call run() method!")
        self.redis: aioredis.Redis = aioredis.from_url(url, decode_responses=True)
        self.stream_wr_queue:asyncio.Queue[Dict_t[str,Any]] = asyncio.Queue()
        self.shutdown = False
        self.last_id = StreamId("$")
        if write_cb is None:
            self._write_cb = self._def_cb
        else:
            self._write_cb = write_cb

    @property
    def connected(self):
        return self._connected

    @connected.setter
    def connected(self,value):
        if self._connected!=value:
            log.warn(f"Redis Client (IN:{self.commands_stream_key}; OUT:{self.output_stream_key}) is now Connected --> {value}")
        self._connected = value

    async def _def_cb(self, command: dict):
        log.warn(f"Got command from stream {self.commands_stream_key}, but not handled (write callback not set!)")
        log.warn(command)

    def setCommandsCallback(self, cb):
        self._write_cb = cb

    def run(self):
        self.ioloop.create_task(self._checkConnection())
        self.ioloop.create_task(self._startStreamWriting())
        self.ioloop.create_task(self._startStreamReading())
        self.ioloop.create_task(self._startTrimming())
        log.info(f"Redis client INPUT_STREAM ({self.commands_stream_key}), OUTPUT_STREAM ({self.output_stream_key}) is running!")

    async def write(self, info: Dict_t[str,Any]):
        """
        A method to be used as a callback to write to redis output Stream
        in format of dict {<domain:key> : val}.

        Uses cache to filter unchanged entries. Hash key is <output_stream>:<domain> key : val.
        """
        await self.stream_wr_queue.put(RedisEntries(info,self.output_stream_key))

    async def writeUnfiltered(self, info: Dict_t[str,Any]):
        """
        A method to be used as a callback to write to redis output Stream
        in format of dict {<domain:key> : val}.
        
        Does not use filtering
        """
        await self.stream_wr_queue.put(RedisEntries(info,self.output_stream_key, filter = False))

    async def _startTrimming(self):
        while True:
            try:
                await asyncio.sleep(5)
                if self.connected:
                    if self.output_stream_key:
                        await self.redis.xtrim(self.output_stream_key, self.max_pub_length)
                    if self.commands_stream_key:
                        await self.redis.xtrim(self.commands_stream_key, self.max_sub_length)
                await asyncio.sleep(60)
            except aioredis.exceptions.ConnectionError:
                self.connected = False
                await asyncio.sleep(1)
                continue
            except asyncio.exceptions.CancelledError:
                raise
            except aioredis.exceptions.ResponseError:
                await asyncio.sleep(60)
                continue
            except:
                log.error("Error occured:")
                log.error(traceback.format_exc())
                raise  

    async def _checkConnection(self):
        num_tries = 0
        while True:
            try:
                await self.redis.ping()
                num_tries = 0
                self.connected = True
                await asyncio.sleep(3)
            except aioredis.exceptions.ConnectionError:
                self.connected = False
                num_tries += 1
                if num_tries>5 and not (num_tries%10):
                    log.error(f"Cannot connect to redis for {num_tries} seconds already")
                await asyncio.sleep(1)
                continue
            except asyncio.exceptions.CancelledError:
                raise
            except:
                log.error("Error occured:")
                log.error(traceback.format_exc())
                raise

    async def shutdownHook(self):
        if not self.shutdown:
            await self.redis.close()
            self.connected = False
            self.shutdown = True

    async def processRedisWrQueue(self, entries_q: asyncio.Queue):
        try:
            result = {}
            hash_key = ""
            cache = {}
            while not entries_q.empty():
                entries:RedisEntries = entries_q.get_nowait()
                curr_hash_keys = entries.hashKeys()
                for curr_hash_key in curr_hash_keys:
                    if hash_key != curr_hash_key:
                        hash_key = curr_hash_key
                        cache = await self.redis.hgetall(hash_key)
                    for key, val in entries.items(curr_hash_key):
                        split_key = key.split(":")
                        field = split_key[-1]
                        current = cache.get(field)
                        if not current is None and current == str(val) and entries.filter:
                            continue
                        await self.redis.hset(hash_key, field, val)
                        cache[field] = val
                        result[key] = val
            return result
        except:
            log.error("An Error occured while processing write request:")
            log.error(traceback.format_exc())

    async def processRedisWrList(self, entries_list: List[dict]):
        result = {}
        if type(entries_list) == dict:
            entries_list = [entries_list]
        while entries_list:
            entries = entries_list.pop()
            for key, val in entries.items():
                result[key] = val
        return result


    @async_oneshot
    @async_handle_exceptions(redis_hadler)
    async def fastStreamWrite(self, entries_list:List[dict]):
        if not self.output_stream_key:
            log.warn("Fast write called without output stream configured!")
            return
        to_write = await self.processRedisWrList(entries_list)
        if to_write:
            log.info(f'Force writing to stream {self.output_stream_key}:')
            log.info(to_write)
            new_entry_id = await self.redis.xadd(self.output_stream_key, to_write)
            log.info(f'Station stream entry added: {new_entry_id}')

    @async_oneshot
    @async_handle_exceptions(redis_hadler)
    async def saveKeyVal(self, key:str, info:str, *, filtered:bool = False):
        if not info is None:
            if filtered:
                current = await self.redis.get(key)
                if str(current) == str(info):
                    return    
            await self.redis.set(key, info)
        else:
            log.warn("SET called without info!")
            await asyncio.sleep(0.05)
            return

    @async_oneshot
    @async_handle_exceptions(redis_hadler)
    async def addToSet(self, set_key:str, *info:list):
        if not info is None and not set_key is None:
            await self.redis.sadd(set_key, *info)
        else:
            log.warn("SADD Append called without info!")
            await asyncio.sleep(0.05)
            return

    @async_oneshot
    @async_handle_exceptions(redis_hadler)
    async def getSetCache(self, set_key:str)->Union[list,None]:
        if set_key:
            return await self.redis.smembers(set_key)
        else:
            log.warn("SMEMBERS called without set_key!")
            await asyncio.sleep(0.05)
            return None

    @async_oneshot
    @async_handle_exceptions(redis_hadler)
    async def addToHash(self, hash_key, info:dict):
        if not info is None:
            await self.redis.hmset(hash_key, info)
        else:
            log.warn("HMSET called without info!")
            await asyncio.sleep(0.05)
            return

    @async_oneshot
    @async_handle_exceptions(redis_hadler)
    async def getHashCache(self, hash_key:str) -> dict:
        if hash_key:
            raw_dict = await self.redis.hgetall(hash_key)
            return raw_dict
        else:
            log.warn("HGETALL called with empty hash_key!")
            await asyncio.sleep(0.05)
            return

    @async_oneshot
    @async_handle_exceptions(redis_hadler)
    async def deleteKey(self, key:str):
        if key:
            await self.redis.delete(key)
        else:
            log.warn("DEL called with empty key!")
            await asyncio.sleep(0.05)
            return

    @async_oneshot
    @async_handle_exceptions(redis_hadler)
    async def deleteSetMembers(self, set:str, *values):
        if not values is None:
            await self.redis.srem(set, *values)
        else:
            log.warn("SREM called without values!")
            await asyncio.sleep(0.05)
            return

    @async_repeating_task(delay=0)
    @async_handle_exceptions(redis_hadler)
    async def _startStreamWriting(self):
        """
        Cached writing using autogenerated hash key.
        
        The hash-key for (device:field) is (output stream key):(device)
        """
        if not self.output_stream_key:
            raise LoopReturn("No output stream given! Leaving task...")
        while not self.stream_wr_queue.empty():
            to_write = await self.processRedisWrQueue(self.stream_wr_queue)
            if to_write:
                log.info(f'Writing to stream {self.output_stream_key}:')
                log.info(to_write)
                new_entry_id = await self.redis.xadd(self.output_stream_key, to_write)
                log.info(f'{self.output_stream_key} stream entry added: {new_entry_id}')
            await asyncio.sleep(0)
        else:
            await asyncio.sleep(0.2)
    
    @async_repeating_task(delay=0)
    @async_handle_exceptions(redis_hadler)
    async def _startStreamReading(self):
        if not self.commands_stream_key:
            raise LoopReturn("No input stream passed --> Aborting reading")
        raw_resp = await self.redis.xread({self.commands_stream_key: self.last_id.raw}, block=BLOCK_DELAY)
        if raw_resp:
            resp = (raw_resp)[0][1:]
        else:
            await asyncio.sleep(self.read_delay)
            raise LoopContinue
        last_id = StreamId()
        for subresp in resp:
            for (raw_current_id, entry) in subresp:
                current_id = StreamId(raw_current_id)
                if current_id > last_id:
                    last_id = current_id
                coro = self._write_cb(entry)
                if coroutines.iscoroutine(coro):
                    await coro 
            self.last_id = last_id

    async def handleTerm(self):
        while not self.stream_wr_queue.empty():
            to_write = await self.processRedisWrQueue(self.stream_wr_queue)
            await self.fastStreamWrite(to_write)

@dataclass
class StreamId:
    raw: str = "0-0"
    time: int = field(init=False)
    seq: int = field(init=False)

    def __post_init__(self):
        if self.raw != "$":
            rsplit = self.raw.split("-")
            self.time =int(rsplit[0])
            self.seq = int(rsplit[1])
    
    def __lt__(self, other) -> bool:
        if self.time < other.time:
            return True
        elif self.time == other.time:
            return self.seq < other.seq
        else:
            return False
    def __le__(self, other) -> bool:
        if self.time < other.time:
            return True
        elif self.time == other.time:
            return self.seq <= other.seq
        else:
            return False
    def __gt__(self, other) -> bool:
        if self.time > other.time:
            return True
        elif self.time == other.time:
            return self.seq > other.seq
        else:
            return False
    def __ge__(self, other) -> bool:
        if self.time > other.time:
            return True
        elif self.time == other.time:
            return self.seq >= other.seq
        else:
            return False
    def __eq__(self, other) -> bool:
        return self.raw == other.raw