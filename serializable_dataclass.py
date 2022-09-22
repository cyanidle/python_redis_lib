from datetime import datetime
from inspect import signature
import logging
from types import NoneType
from typing import Any, Callable, List, Tuple, Type, Union, get_args, get_origin
from typing import Dict
from dataclasses import MISSING, Field, asdict
from dataclasses import fields as datacls_fields

log = logging.getLogger("serialization")

class MissingRequiredValueError(RuntimeError):
    pass


class SerializableDataclass:
    @classmethod
    def deserialise(cls, src_dict:dict, *, deserialise_rules: Dict[Type, Callable] = {}, pedantic = True):
        def check_type_default(value:Union[dict, Any, list], wanted_type: Union[Type[datetime], Type[SerializableDataclass], Type[float]]) -> Any:
            if get_origin(wanted_type) is dict:
                try:
                    result = {}
                    wanted_key_type, wanted_val_type = get_args(wanted_type)
                    for key, val in value.items():
                        actual_key = check_type(key, wanted_key_type)
                        actual_value = check_type(val, wanted_val_type)
                        result[actual_key] = actual_value
                        return result
                except Exception as e:
                    log.warn("Dont forget to specify Dict[cls, cls]")
                    log.error(f"Error deserialising {cls}. Source is not a dictionary!")
                    raise(e)
            elif get_origin(wanted_type) is list:
                try:
                    result = []
                    wanted_type = get_args(wanted_type)[0]
                    for val in value:
                        result.append(check_type(val, wanted_type))
                    return result
                except Exception as e:
                    log.warn("Dont forget to specify List[cls]")
                    log.error(f"Error deserialising {cls}. Source is not a list!")
                    raise(e)
            elif get_origin(wanted_type) is Union:
                actual_types = get_args(wanted_type)
                for actual_type in actual_types:
                    try:
                        return check_type(value, actual_type)
                    except:
                        continue
                log.warn(f"Not a single Union type could be used to deserialise {value}")
                raise TypeError("Not a single Union type could be used to deserialise")
            elif get_origin(wanted_type) is Tuple:
                result = []
                for actual_value, actual_type in zip(value, get_args(wanted_type)):
                    result.append(check_type(actual_value, actual_type))
                return tuple(result)
            elif issubclass(wanted_type, SerializableDataclass):
                return wanted_type.deserialise(value, pedantic=pedantic)
            elif wanted_type is NoneType:
                return None
            elif wanted_type is Any:
                return value
            else:
                try:
                    return wanted_type(value)
                except: 
                    raise TypeError(f"Could not convert (value: '{value}' of type: '{type(value)}') to type {wanted_type}")
        ###
        def check_type(value:str, type: Union[Type[datetime], Type[SerializableDataclass], Type[float]]) -> Any:
            if not deserialise_rules.get(type) is None:
                return deserialise_rules.get(type)(value)
            else:
                return check_type_default(value, type)
        ###
        fields:Tuple[Field,...] = datacls_fields(cls)
        for field in fields:
            if not field.init:
                continue
            should_skip = False
            try:
                raw_value = src_dict[field.name]
            except KeyError:
                if field.default is MISSING and field.default_factory is MISSING:
                    raise MissingRequiredValueError(f"Missing mandatory value '{field.name}' for deserialisation of class: {cls.__name__}")
                else:
                    should_skip = True
            if not should_skip:
                src_dict[field.name] = check_type(raw_value, field.type)
        result = cls.from_kwargs(**src_dict, pedantic=pedantic)
        return result

    def _serialise_impl(self, src: List[Tuple[str, Any]], *, rules = Dict[Type, Callable[[Any], Any]]):
        log.info(src)
        pass

    def serialise(self) -> Dict:
        return asdict(self, dict_factory=self._serialise_impl)

    @classmethod
    def from_kwargs(cls, pedantic = True, **kwargs):
        cls_fields = [field for field in signature(cls).parameters]
        native_args, new_args = {}, {}
        for name, val in kwargs.items():
            if name in cls_fields:
                native_args[name] = val
            else:
                new_args[name] = val
        try:
            ret = cls(**native_args)
            if pedantic and new_args:
                log.warn(f"Extra arguments passed for deserialisation of ({cls.__name__}): {new_args}")
            return ret
        except TypeError as e:
            log.error(f"Could not parse class: {cls.__name__}")
            log.error(f"Reason: {e}")
            raise TypeError(f"Not enough arguments passed: (Passed: {native_args}; Needed: {cls_fields})")