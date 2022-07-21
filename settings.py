# -*- coding: utf-8 -*-
import traceback
import toml
from pymodbus.client.asynchronous.async_io import AsyncioModbusSerialClient as ModbusClientSerial
from pymodbus.client.asynchronous.async_io import AsyncioModbusUdpClient as ModbusClientUDP
from pymodbus.client.asynchronous.async_io import AsyncioModbusTcpClient as ModbusClientTCP
from pymodbus.framer.rtu_framer import ModbusRtuFramer as RtuFramer
from pymodbus.framer.socket_framer import ModbusSocketFramer as SocketFramer
from pymodbus.client.asynchronous import schedulers
import calendar as cldr
import datetime as dt
import base64
from math import sin
from aioping import ping
import asyncio
from os import path, stat
from typing import Any, Union
from typing import Tuple as Tuple_t
from typing import List as List_t
from typing import Dict as Dict_t
from dataclasses import KW_ONLY, dataclass, field
import logging
import logging.handlers
log = logging.getLogger()

@dataclass (slots=True)
class ConnectionSettings:
    host: str = "0.0.0.0"
    port: int = 502

@dataclass (slots=True)
class ModbusDevice:
    parent: object = None
    parent_name: str = ""
    hash_name: str = ""
    slave_id: int = 0
    name: str = ""
    stop_commands: dict = field(default_factory=lambda:dict())

    holding_ranges: List_t[Tuple_t[int,int]] = field(default_factory = lambda: list())
    input_ranges: List_t[Tuple_t[int,int]] = field(default_factory = lambda: list())

    holding_registers: Dict_t[str,int] = field(default_factory = lambda: dict())
    reversed_holding: Dict_t[int,str] = field(init=False)

    input_registers: Dict_t[str,int]  = field(default_factory = lambda: dict())
    reversed_input: Dict_t[int,str] = field(init=False)

    write_registers: Dict_t[str,int] = field(default_factory = lambda: dict())
    reversed_write:Dict_t[int,str] = field(init=False)

    hold_polling_waittime: float = 3
    input_polling_waittime: float = 1
    write_delay: float = 0.5
    write_queue: asyncio.Queue = field(init=False)

    def initReversedDicts(self):
        def flipDict(source:dict)->dict:
            result = dict()
            for key, val in source.items():
                result[val] = key
            return result
        self.reversed_holding = flipDict(self.holding_registers)
        self.reversed_input = flipDict(self.input_registers)
        self.reversed_write = flipDict(self.write_registers)

    async def _processHolding(self, registers: list, start_index: int) -> dict:
        if not registers or registers is None:
            return dict()
        result = dict()
        for num, register in enumerate(registers):
            full_index = num + start_index
            key = self.reversed_holding.get(full_index)
            if key and key is not None:
                result[f"{self.parent.settings.name}:{self.name}:{key}"] = register
        return result

    async def _processInput(self, registers, start_index: int) -> dict:
        if not registers or registers is None:
            return dict()
        result = dict()
        for num, register in enumerate(registers):
            full_index = num + start_index
            key = self.reversed_input[full_index]
            result[f"{self.parent.settings.name}:{self.name}:{key}"] = register
        return result

    async def _processWriteData(self, data:dict) -> Union[tuple,None]:
        for key, val in data.items():
            return (self.write_registers[key], int(val))
    
    @property
    def full_name(self):
        return f"{self.parent_name}:{self.name}"

class RedisEntries:
    def __init__(self, raw_dict:Dict_t[str,Union[str,int]], stream_id : str, *, filter = True) -> None:
        if type(raw_dict) != dict:
            log.error(f"Redis Entries cannot be initialised from non-dict entries! ({raw_dict})")
            raise TypeError
        self.raw_dict = raw_dict
        self.stream_id = stream_id
        self.filter = filter
        #base_key = ""
        # try:
        #     for key in self.raw_dict:
        #         curr_key = key.split(":")[:-1]
        #         if not base_key:
        #             base_key = curr_key
        #         if base_key!=curr_key:
        #             log.warn(f"Non homogenous hash key while creating Redis Entries! ({base_key} != {curr_key})")
        #             log.warn("Only dict with entries for ONE object (same key, different fields) should be passed!")
        # except:
        #     pass

    def hashKeys(self) -> List_t[str]:
        result = []
        for key in self.raw_dict:
            curr_key = self._hashKey(key)
            if not curr_key in result:
                result.append(curr_key)
        return result

    def _hashKey(self,key:str):
        try: 
            return self.stream_id + ':' + key[:key.rindex(':')]
        except ValueError:
            return self.stream_id + ':' +key

    def items(self, hash_key:str = None):
        result = {}
        if hash_key:
            for key,val in self.raw_dict.items():
                if self._hashKey(key) == hash_key:
                    result[key] = val
            return result.items()
        return self.raw_dict.items()
@dataclass (slots=True)
class BolidSettings:
    connection: ConnectionSettings = ConnectionSettings()
    client_type: str = "udp"
    framer_type: str = "rtu"
    framer_factory: Union[RtuFramer,SocketFramer] = RtuFramer
    client_factory: Union[ModbusClientTCP, ModbusClientUDP] = ModbusClientUDP
    devices: List_t[ModbusDevice] = field(default_factory = lambda: list())
    name: str = ""
    reconnect_timeout: int = 5
    query_handle_delay: float = 0.02
    query_timeout: float = 0.5
    max_queries:int = 50
    
@dataclass (slots=True)
class RedisSettings:
    commands_stream: str = ""
    output_stream: str = ""
    host: str = "127.0.0.1"
    port: int = 6300
    write_delay: float = 1
    read_delay: float = 0.5
    max_pub_length: int = 50000
    max_sub_length: int = 50000 


@dataclass (slots=True)
class NavigardServerSettings:
    connection: ConnectionSettings
    contact_id_map: Dict_t[str,str]
    commands_map: List_t[dict]
    KW_ONLY
    command_timeout: float = 1
    commands_prefix: str = None 

    def __post_init__(self):
        for command in self.commands_map:
            current = self.commands_map[command]
            if type(current) != list:
                self.commands_map[command] = [current]

@dataclass (slots=True)
class OperationMode:
    name: str
    time_per_step: float
    step: float 
    min_speed: float
    raw_data: List_t[int]
    data: List_t[int] = field(init=False)
    current_index: int = field(init=False, default=-1)

    def __post_init__(self):
        self.data = []
        for perc in self.raw_data:
            if perc > 100:
                current = int(round(self.step * 100))
            elif perc*self.step < self.min_speed*self.step:
                current = 0
            else:
                current = int(round(perc * self.step))
            self.data.append(current)

    async def getNext(self) -> int:
        await asyncio.sleep(self.time_per_step)
        self.current_index+=1
        if self.current_index >= len(self.data):
            self.current_index = 0
        return self.data[self.current_index]
    def reset(self):
        self.current_index = -1

@dataclass (slots=True)
class UptimeSettings:
    days: List_t[int]
    time_ranges: List_t[Tuple_t[dt.time, dt.time]]

    def is_up(self,check_time=None, now_day=None ) -> bool:
        # If check time is not given, default to current UTC time
        now_day = now_day or dt.datetime.today().weekday()
        check_time = check_time or dt.datetime.now().time()
        ranges_result = [False]
        for start, end in self.time_ranges:
            if start < end:
                ranges_result.append(check_time >= start and check_time <= end) 
            else: # crosses midnight
                ranges_result.append(check_time >= start or check_time <= end)
        day_allowed = now_day in self.days
        return any(ranges_result) and day_allowed

@dataclass (slots=True)
class ModbusControlSet:
    target_device: str
    spd_register:str
    on_raw:str
    off_raw:str
    on_field:str = field(init=False)
    on_value:int = field(init=False)
    off_field:str = field(init=False)
    off_value:int = field(init=False)

    def __post_init__(self):
        on_raw = self.on_raw.split("/")
        self.on_field = on_raw[0]
        self.on_value = on_raw[1]
        off_raw = self.off_raw.split("/")
        self.off_field = off_raw[0]
        self.off_value = off_raw[1]

    @property
    def speed_key(self):
        return f"{self.target_device}:{self.spd_register}"

    @property
    def on_key(self):
        return f"{self.target_device}:{self.on_field}"

    @property
    def off_key(self):
        return f"{self.target_device}:{self.off_field}"

@dataclass (slots=True)
class FountainSettings:
    name:str
    control_set:ModbusControlSet
    uptime: UptimeSettings = field(init=False)
    op_modes: Dict_t[str,OperationMode] = field(init=False)
    redis: RedisSettings = field(init=False)

@dataclass (slots=True)
class ManagerSettings:
    ping_delay: float = 1
    queue_flush_check_delay: float = 2
    max_queue: int = 5000

class Reader:

    framer_factory_dict = {"rtu" : RtuFramer, "socket": SocketFramer}
    client_factory_dict = {"udp": ModbusClientUDP, "tcp": ModbusClientTCP, "serial": ModbusClientSerial}

    def __init__(self, *, dir = "conf", file = "config.toml") -> None:
        self.config_directory = dir
        self.config_file = file
        self.read(force=True)

    def configPath(self, file: str = None):
        if not file:
            file = self.config_file
        return f"{self.config_directory}/{file}"

    def setDirectory(self, directory: str):
        if path.exists(directory):
            Reader.config_directory = directory
        else:
            Reader.log.warn("Directory passed is nonexistent")
        try:
            self.read(force=True)
        except:
            Reader.log.warn("New directory doesnt contain valid files")
            Reader.log.error(traceback.format_exc())

    def setConfigFile(self, file : str):
        if path.exists(self.configPath(file)):
            self.config_file = file
        else:
            log.warn("File passed is nonexistent")
        try:
            self.read(force=True)
        except:
            Reader.log.warn("New config file isnt valid")
            Reader.log.error(traceback.format_exc())

    def read(self, force: bool):
        if force or not self.config_dict:
            try:
                self.config_dict = self._read()
            except:
                log.warn(f"{self.configPath()} not found!")
    def _read(self, file:str = None):
        with open(self.configPath(file),"r") as f:
                return toml.load(f)

    def parseModbusDevices(self, devs: List_t[dict], *,  parent_settings: BolidSettings, registers_file = "config.toml") -> List_t[ModbusDevice]:
        def parseRegisters(device:ModbusDevice, ranges: List_t[dict], registers_file = "config.toml") -> None:
            if ranges:
                for reg_ranges in ranges:
                    if reg_ranges["type"] == "holding":
                        device.holding_ranges.append((reg_ranges["reg_index"], reg_ranges["reg_count"]))
                    elif reg_ranges["type"] == "input":
                        device.input_ranges.append((reg_ranges["reg_index"], reg_ranges["reg_count"]))
                    else:
                        raise RuntimeError(f"Incorrect type passed in settings {reg_ranges['type']}")
            ########
            registers_dict = self._read(registers_file)
            holdings_dict = registers_dict.get("registers").get(parent_settings.name).get(device.name, {}).get("holding", {})
            inputs_dict = registers_dict.get("registers").get(parent_settings.name).get(device.name, {}).get("input", {})
            write_dict = registers_dict.get("registers").get(parent_settings.name).get(device.name, {}).get("write", {})
            def fillDict(dict_to_fill:dict, source:dict):
                for key in source:
                    dict_to_fill[key] = source[key]["index"]
            fillDict(device.holding_registers,holdings_dict)
            fillDict(device.input_registers,inputs_dict)
            fillDict(device.write_registers,write_dict) 
            device.initReversedDicts()
        ###############################################
        result = list()
        for name, mod_dict in devs.items():
            current = ModbusDevice()
            current.name = name
            current.slave_id = mod_dict.get("slave_id", current.slave_id)
            current.hold_polling_waittime = mod_dict.get("holding_poll_time", current.hold_polling_waittime)
            current.input_polling_waittime = mod_dict.get("input_poll_time", current.input_polling_waittime)
            current.write_delay = mod_dict.get("write_delay", current.write_delay)
            current.stop_commands = mod_dict.get("stop_commands", current.stop_commands)
            current.parent_name = parent_settings.name
            current.write_queue = asyncio.Queue(parent_settings.max_queries)
            parseRegisters(current, mod_dict.get("register_ranges"),registers_file)
            result.append(current)
        return result
    
    def parseBolids(self, **kwargs) -> List_t[BolidSettings]:
        result = list()
        for bol_name in self.config_dict["bolid"]:
            current_bol_dict = self.config_dict["bolid"][bol_name]
            current = BolidSettings()
            current.name = bol_name
            current.query_handle_delay = current_bol_dict.get("query_handle_delay", current.query_handle_delay)
            current.query_timeout = current_bol_dict.get("query_timeout", current.query_timeout)
            current.max_queries = current_bol_dict.get("max_queries", current.max_queries)
            current.connection.port = current_bol_dict.get("port", current.connection.port)
            current.connection.host = current_bol_dict.get("host", current.connection.host)
            current.client_type = current_bol_dict.get("connection_type", current.client_type)
            current.framer_type = current_bol_dict.get("protocol", current.framer_type)
            current.framer_factory = Reader.framer_factory_dict.get(current.framer_type)
            current.client_factory = Reader.client_factory_dict.get(current.client_type)
            current.devices = self.parseModbusDevices(current_bol_dict.get("devices",{}), parent_settings=current, registers_file=kwargs.get("registers_file"))
            result.append(current)
        return result

    def parseManagerSettings(self) -> ManagerSettings:
        result = ManagerSettings()
        manager_dict = self.config_dict.get("manager").get("settings")
        result.queue_flush_check_delay = manager_dict.get("queue_flush_check_delay", result.queue_flush_check_delay)
        result.ping_delay = manager_dict.get("ping_delay", result.ping_delay)
        return result

    def parseRedisSettings(self, *, top_dict:dict = None) -> RedisSettings:
        result = RedisSettings()
        if top_dict:
            current_dict = top_dict
        else:
            current_dict:dict = self.config_dict
        redis_dict = current_dict.get("redis")
        result.commands_stream = redis_dict.get("commands_stream")
        result.write_delay = redis_dict.get("write_delay")
        result.read_delay = redis_dict.get("read_delay")
        result.output_stream = redis_dict.get("output_stream")
        result.host = redis_dict.get("host")
        result.port = redis_dict.get("port")
        result.max_pub_length = int(redis_dict.get("max_publish_length",result.max_pub_length))
        result.max_sub_length = int(redis_dict.get("max_commands_length",result.max_sub_length))
        return result

    def parseControlSet(self, src: Dict_t[str,str]) -> ModbusControlSet:
        return ModbusControlSet(
            src.get("target_device"),
            src.get("speed_key"),
            src.get("on_command"),
            src.get("off_command"),
        )        

    def parseFountains(self) -> List_t[FountainSettings]:
        fountain_dict = self.config_dict.get("fountain")
        full_result = []
        for fountain_name, sub_dict in fountain_dict.items():
            result = FountainSettings(
                fountain_name,
                self.parseControlSet(sub_dict.get("control_set"))
            )
            op_modes:Dict_t[str,OperationMode] = {}
            for op_mode, op_dict  in sub_dict.get("operation").items():
                current_op_mode = OperationMode(
                    op_mode,
                    op_dict.get("time_per_step"),
                    op_dict.get("step"),
                    op_dict.get("min_speed"),
                    op_dict.get("data")
                )
                op_modes[op_mode] = current_op_mode
            result.op_modes = op_modes
            result.redis = self.parseRedisSettings(top_dict=sub_dict)
            result.uptime = self.parseUptime(sub_dict.get("uptime"))
            full_result.append(result)
        return full_result

    def parseUptime(self, src: Dict_t[str, Any]) -> UptimeSettings:
        days = src.get("days")
        ranges: List_t[Tuple_t[dt.time, dt.time]] = []
        times_list : List_t[dict] = src.get("ranges")
        for times_dict in times_list:
            time_on: List_t[str] = times_dict.get("start").split(":")
            time_off: List_t[str] = times_dict.get("end").split(":")
            ranges.append((dt.time(int(time_on[0]),int(time_on[1])),
                            dt.time(int(time_off[0]),int(time_off[1]))))
        return UptimeSettings(
            days, ranges
        ) 

    def parseNavigardServer(self, src: Dict_t[str, Any] = None) -> NavigardServerSettings:
        if src is None:
            src = self.config_dict.get("navigard_server")
        try:
            host = src.get("connection").get("host")
            port = src.get("connection").get("port")
        except:
            log.error("Connection settings for Navigard Server not provided!")
            raise
        contact_id_mapping = src.get("mappings").get("contact_id")
        command_timeout = src.get("command_timeout")
        commands_prefix  = src.get("commands_prefix")
        commands_map = src.get("mappings").get("commands")
        connection = ConnectionSettings(host, port)
        return NavigardServerSettings(connection, contact_id_mapping, commands_map,
                                     command_timeout=command_timeout, commands_prefix=commands_prefix)

    