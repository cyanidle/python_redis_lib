# -*- coding: utf-8 -*-
from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
import os
import traceback
from types import NoneType
from typing_extensions import Self
import toml
from os import path
from typing import Any, Callable, Type, Union
from typing import List
from typing import Dict
import logging
import logging.handlers
from inspect import signature
from mergedeep import merge
from .serializable_dataclass import MissingRequiredValueError, SerializableDataclass

log = logging.getLogger("settings")

def parse_time_to_seconds(src:str) -> int:
    """
    Converts "h:m:s" to seconds ("1:1:1" -> 1 * 3600 + 1*60 + 1)
    """
    try:
        split_rate = src.split(":")
        return 3600 * int(split_rate[0]) + 60 * int(split_rate[1]) + int(split_rate[2])
    except:
        log.warn(f"Unable to parse --> ({src}). Returning 1 hour.")
        return 3600

def apply_to_nested_values(src:dict, func):
    for _, val in src.items():
        if isinstance(val, dict):
            apply_to_nested_values(val, func)
        else:
            val = func(val)

def flatten_dict(source: dict, *, parent_key: str = '', sep: str =':') -> Dict[str, Any]:
    items = []
    for k, v in source.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, parent_key = new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
 
def nest_dict(source_dict: Dict[str, str], *, sep = ":") -> dict:
    def _traverse_keys_into_dict(keys: Union[str, List[str]], val) -> dict:
        if isinstance(keys, list):
            if len(keys) > 1:
                return{keys[0]: _traverse_keys_into_dict(keys[1:], val)}
            else:
                keys = keys[0]
        return {keys: val}

    all_args = {}
    for key, val in source_dict.items():
        split_key = key.split(sep)
        if not split_key:
            continue
        if len(split_key) == 1:
            all_args[split_key[0]] = val
            continue
        merge(all_args, _traverse_keys_into_dict(split_key, val))
    return all_args



class RedisEntries:
    def __init__(self, raw_dict:Dict[str,Union[str,int]], stream_id : str, *, filter = True) -> None:
        if type(raw_dict) != dict:
            log.error(f"Redis Entries cannot be initialised from non-dict entries! ({raw_dict})")
            raise TypeError
        self.raw_dict = raw_dict
        self.stream_id = stream_id
        self.filter = filter

    def hashKeys(self) -> List[str]:
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

class CachedSerialiazable(SerializableDataclass):
    
    _deserialised_cache: Dict[str, Self] = {}

    @abstractmethod
    def _name(self) -> str:
        raise NotImplementedError

    @classmethod
    def deserialise(cls, src_dict: dict, *, deserialise_rules: Dict[Type, Callable] = {}, pedantic=True):
        wanted_name = src_dict.get("name") 
        if wanted_name is not None and wanted_name in cls._deserialised_cache:
            return cls._deserialised_cache[wanted_name]
        try:
            result:CachedSerialiazable = super().deserialise(src_dict, deserialise_rules=deserialise_rules, pedantic=pedantic)
        except MissingRequiredValueError:
            raise MissingRequiredValueError(f"No cached object with name: '{wanted_name}' found! Remember to precache servers and etc.")
        cls._deserialised_cache[result._name()] = result
        return result

@dataclass(slots=True, kw_only=True)
class ConnectionSettings(CachedSerialiazable):
    name: str
    host: str 
    port: int 
    def _name(self) -> str:
        return self.name

@dataclass (slots=True, kw_only=True)
class RedisSettings(SerializableDataclass):
    server: ConnectionSettings
    commands_stream: str = ""
    output_stream: str = ""
    max_pub_length: int = 50000
    max_sub_length: int = 50000 
    @property
    def host(self):
        return self.server.host
    @property
    def port(self):
        return self.server.port

@dataclass (slots=True, kw_only=True)
class LoggingSettings(SerializableDataclass):
    levels:dict = field(default_factory=dict)
    add_timestamp:bool = False
    enable_console:bool = True
    logfilename:str = "" #empty = do not use
    enable_function_name: bool = False

@dataclass (slots=True, kw_only=True)
class SqlClientSettings(SerializableDataclass):
    server: ConnectionSettings
    user_env: str
    password_env: str = ""
    db: str
    user: str = field(init=False)
    password :str = field(init=False)
    @property
    def host(self):
        return self.server.host
    @property
    def port(self):
        return self.server.port
    def __post_init__(self):
        self.user = get_env_value(self.user_env)
        if self.password_env:
            self.password = get_env_value(self.password_env)


def get_env_value(name:str, *, filepath:str = ".env"):
    try:
        with open(filepath, "r") as f:
            for line in f.readlines():
                split_line = line.split("=")    
                if split_line[0] == name:
                    text_token = split_line[1]
                    break
    except:
        log.warn(f"Could not load env value {name} from {filepath} file")
    return os.getenv(name) or text_token

class Reader:
    def __init__(self, *, dir = "conf", file = "config.toml") -> None:
        self.config_directory = dir
        self.config_file = file
        self.read(force=True)

    def config_path(self, file: str = None):
        if not file:
            file = self.config_file
        return f"{self.config_directory}/{file}"

    def set_directory(self, directory: str):
        if path.exists(directory):
            Reader.config_directory = directory
        else:
            log.warn("Directory passed is nonexistent")
        try:
            self.read(force=True)
        except:
            log.warn("New directory doesnt contain valid files")
            log.error(traceback.format_exc())

    def set_config_file(self, file : str):
        if path.exists(self.config_path(file)):
            self.config_file = file
        else:
            log.warn("File passed is nonexistent")
        try:
            self.read(force=True)
        except:
            log.warn("New config file isnt valid")
            log.error(traceback.format_exc())

    def read(self, force: bool):
        if force or not self._config_dict:
            try:
                self._config_dict = self._read()
            except:
                log.warn(f"Error reading {self.config_path()}!")
    def _read(self, file:str = None):
        with open(self.config_path(file),"r") as f:
                return toml.load(f)

    def parse(self, struct: Type[SerializableDataclass], key:str = None, pedantic = True, **kwargs) -> SerializableDataclass:
        """
        Used to call on struct class, which contains parse() method.

        Passes full file dictionary as the first argument
        """
        if not issubclass(struct, SerializableDataclass):
            raise RuntimeError("Can parse only 'SerializableDataclass' subclasses!")
        else:
            if key:
                return struct.deserialise(self._config_dict[key], pedantic = pedantic, **kwargs)
            else:
                return struct.deserialise(self._config_dict, pedantic = pedantic, **kwargs)

    def precache(self, struct: Type[CachedSerialiazable], key:str = None, pedantic = True, **kwargs) -> CachedSerialiazable:
        """
        Precache objects from toml! Should be under [[key]] fields!
        """
        if not issubclass(struct, CachedSerialiazable):
            raise RuntimeError("Can parse only 'SerializableDataclass' subclasses!")
        else:
            if key:
                src_list = self._config_dict[key]
            else:
                src_list = self._config_dict
            if type(src_list) is not list:
                raise RuntimeError(f"Cannot precache non list entries!")
            for src in src_list:
                struct.deserialise(src, pedantic = pedantic, **kwargs)

    @property
    def parsed(self) -> dict:
        try:
            return self._config_dict
        except:
            log.error(f"Could not parse settings for {self.config_path()}")
            raise RuntimeError(f"Could not parse settings for {self.config_path()}")