from copy import deepcopy
from datetime import datetime
from functools import partial
from inspect import signature
import logging
from typing import Any, Callable, List, Tuple, Type, Union, get_args, get_origin
from typing import Dict
from dataclasses import MISSING, Field, asdict, dataclass, is_dataclass
from dataclasses import fields as datacls_fields
from typing_extensions import Self

log = logging.getLogger("serialization")

__all__ = ("MissingRequiredValueError", "SerializableDataclass")

class MissingRequiredValueError(RuntimeError):
    pass

@dataclass(slots=True)
class SerializableDataclass:
    @classmethod
    def deserialise(cls, src_dict:dict, *, deserialise_rules: Dict[type, Callable[[Any], Any]] = {}, pedantic = True) -> Self:
        tmp_dict = deepcopy(src_dict)
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
                    log.error(f"Error deserialising {cls}. Source is not a dictionary!")
                    log.warn("Dont forget to specify Dict[cls, cls]")
                    raise(e)
            elif get_origin(wanted_type) is list:
                try:
                    result = []
                    wanted_type = get_args(wanted_type)[0]
                    for val in value:
                        result.append(check_type(val, wanted_type))
                    return result
                except Exception as e:
                    log.error(f"Error deserialising {cls}. Source is not a list!")
                    log.warn("Dont forget to specify List[cls]")
                    raise(e)
            elif get_origin(wanted_type) is Union:
                actual_types = get_args(wanted_type)
                has_none = False
                if type(None) in actual_types:
                    actual_types = tuple(filter(lambda type_in_union: type_in_union!=type(None), actual_types))
                    has_none = True
                for actual_type in actual_types:
                    try:
                        return check_type(value, actual_type)
                    except:
                        continue
                if has_none:
                    if pedantic and value is not None:
                        log.warn(f"Union: While deserialising '{value}' --> Only option: None")
                    return None
                log.warn(f"Not a single Union type could be used to deserialise '{value}'")
                raise TypeError("Not a single Union type could be used to deserialise")
            elif get_origin(wanted_type) is Tuple:
                result = []
                for actual_value, actual_type in zip(value, get_args(wanted_type)):
                    result.append(check_type(actual_value, actual_type))
                return tuple(result)
            elif issubclass(wanted_type, SerializableDataclass):
                return wanted_type.deserialise(value, deserialise_rules=deserialise_rules, pedantic=pedantic)
            elif wanted_type is Any or wanted_type is object:
                return value
            else:
                try:
                    return wanted_type(value)
                except: 
                    raise TypeError(f"Could not convert (value: '{value}' of type: '{type(value)}') to type {wanted_type}")
        ###
        def check_type(value:str, type: Union[Type[datetime], Type[SerializableDataclass], Type[float]]) -> Any:
            if type in deserialise_rules:
                return deserialise_rules[type](value)
            else:
                return check_type_default(value, type)
        ###
        if not is_dataclass(cls):
            raise TypeError(f"Deserialising {cls.__name__}: SerializableDataclass subclasses must be dataclasses themselves!")
        fields:Tuple[Field,...] = datacls_fields(cls)
        for field in fields:
            if not field.init:
                if pedantic and field.name in tmp_dict:
                    log.warn(f"Deserialising {cls.__name__}: Skipping non-init field '{field.name}'")
                continue
            should_skip = False
            try:
                raw_value = tmp_dict[field.name]
            except KeyError:
                if field.default is MISSING and field.default_factory is MISSING:
                    raise MissingRequiredValueError(f"Deserialising {cls.__name__}: Missing mandatory value '{field.name}'")
                else:
                    if pedantic:
                        log.warn(f"Deserialising {cls.__name__}: Using default value for field '{field.name}'")
                    should_skip = True
            except TypeError:
                raise TypeError(f"Deserialising {cls.__name__}: Error, while parsing nested Dataclass '{field.type}': non-dict source")
            if not should_skip:
                tmp_dict[field.name] = check_type(raw_value, field.type)
        result = cls.from_kwargs(**tmp_dict, pedantic=pedantic)
        return result

    def _serialise_impl(self, src: List[Tuple[str, Any]], *, rules: Dict[Type, Callable[[Any], Any]] = {}) -> dict:
        result = {}
        for pair in src:
            value_type = type(pair[1])
            if value_type in rules:
                result[pair[0]] = rules[value_type](pair[1])
            else:
                result[pair[0]] = pair[1]
        return result

    def serialise(self, rules: Dict[Type, Callable[[Any], Any]] = None) -> Dict:
        if rules:
            factory = partial(self._serialise_impl, rules = rules)
        else:
            factory = self._serialise_impl
        return asdict(self, dict_factory=factory)

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