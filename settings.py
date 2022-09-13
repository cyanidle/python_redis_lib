# -*- coding: utf-8 -*-
import traceback
import toml
from abc import ABC
from os import path
from typing import Union
from typing import List as List_t
from typing import Dict as Dict_t
from dataclasses import dataclass, field
import logging
import logging.handlers
from inspect import signature

log = logging.getLogger("settings")

class SettingsStructTemplate(ABC):
    @classmethod
    def parse(cls, src_dict:dict, *args, **kwargs):
        return cls.from_kwargs(**src_dict)        
    @classmethod
    def from_kwargs(cls, **kwargs):
        cls_fields = {field for field in signature(cls).parameters}
        native_args, new_args = {}, {}
        for name, val in kwargs.items():
            if name in cls_fields:
                native_args[name] = val
            else:
                new_args[name] = val
        try:
            ret = cls(**native_args)
            return ret
        except TypeError as e:
            log.error(f"Could not parse class: {cls.__name__}")
            log.error(f"Reason: {e}")


@dataclass (slots=True)
class ConnectionSettings:
    host: str = "0.0.0.0"
    port: int = 0

class RedisEntries:
    def __init__(self, raw_dict:Dict_t[str,Union[str,int]], stream_id : str, *, filter = True) -> None:
        if type(raw_dict) != dict:
            log.error(f"Redis Entries cannot be initialised from non-dict entries! ({raw_dict})")
            raise TypeError
        self.raw_dict = raw_dict
        self.stream_id = stream_id
        self.filter = filter

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
class RedisSettings(SettingsStructTemplate):
    commands_stream: str = ""
    output_stream: str = ""
    host: str = "127.0.0.1"
    port: int = 6300
    max_pub_length: int = 50000
    max_sub_length: int = 50000 

@dataclass (slots=True, kw_only=True)
class LoggingSettings(SettingsStructTemplate):
    levels:dict = field(default_factory=lambda:dict())
    add_timestamp:bool = False
    enable_console:bool = True
    logfilename:str = "" #empty = do not use
    enable_function_name: bool = False

class Reader:
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
                log.warn(f"Error reading {self.configPath()}!")
    def _read(self, file:str = None):
        with open(self.configPath(file),"r") as f:
                return toml.load(f)

    def parseRedisSettings(self, *, top_dict:dict = None) -> RedisSettings:
        result = RedisSettings()
        if top_dict:
            current_dict = top_dict
        else:
            current_dict:dict = self.config_dict
        redis_dict = current_dict.get("redis")
        result.commands_stream = redis_dict.get("commands_stream")
        result.output_stream = redis_dict.get("output_stream")
        result.host = redis_dict.get("host")
        result.port = redis_dict.get("port")
        result.max_pub_length = int(redis_dict.get("max_publish_length",result.max_pub_length))
        result.max_sub_length = int(redis_dict.get("max_commands_length",result.max_sub_length))
        return result

    def parseLogging(self) -> LoggingSettings:
        src:dict = self.config_dict.get("logging")
        if src is None:
            return None
        levels = src.get("levels", dict())
        result =  LoggingSettings()
        result.levels = levels
        result.add_timestamp = src.get("add_timestamp", result.add_timestamp)
        result.enable_console = src.get("enable_console", result.enable_console)
        result.logfilename = src.get("logfilename", result.logfilename)
        result.enable_function_name= src.get("enable_function_name", result.enable_function_name)
        return result

    def parse(self, struct: SettingsStructTemplate, *args, key:str = None, **kwargs):
        """
        Used to call on struct class, which contains parse() method.

        Passes full file dictionary as the first argument
        """
        if not hasattr(struct, "parse") or not callable(struct.parse):
            log.error(f"Passed class does not contain parse() method!")
        else:
            if key:
                return struct.parse(self.config_dict[key], *args, **kwargs)
            else:
                return struct.parse(self.config_dict, *args, **kwargs)