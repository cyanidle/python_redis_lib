# -*- coding: utf-8 -*-
import logging
import logging.handlers
import os.path
from typing import List
from .settings import LoggingSettings

log_root = logging.getLogger()
log = logging.getLogger("init-logging")

def initLogging(caller_file:str, *,  settings: LoggingSettings = LoggingSettings()) -> None:
    """
    Should be called with '__file__' as the first argument and 'module name' as in logging config as second!
    """
    handlers:List[logging.Handler] = []
    format_str = '%(levelname)-8s %(name)-12s %(lineno)-6s %(message)s'
    if settings.add_timestamp:
        format_str = '%(asctime)-15s ' + format_str
    if settings.enable_function_name:
        format_str += '   <-- %(funcName)s()'
    format = logging.Formatter(format_str)
    if settings.logfilename:
        logfilename = os.path.join(os.path.dirname(os.path.realpath(caller_file)), *(settings.logfilename.split("/")))
        file_handler = logging.handlers.RotatingFileHandler(
            logfilename, maxBytes = 524288000, backupCount = 2)
        file_handler.setFormatter(format)
        handlers.append(file_handler)
    if settings.enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(format)
        handlers.append(console_handler)
    if "root" in settings.levels.keys():
        try:
            root_level = _levels_dict[settings.levels.pop("root")]
        except:
            root_level = logging.INFO    
    else:
        root_level = logging.INFO
    log_root.setLevel(root_level)
    for handler in handlers:
        handler.setLevel(root_level)
        log_root.addHandler(handler)
    for name, val in settings.levels.items():
        _set_level(name, val)

_levels_dict = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR
}

def _set_level(name:str, value:str):
    try:
        logging.getLogger(name).setLevel(_levels_dict[value])
        log.warn(f"Setting logger level for {name} --> {value}")
    except KeyError:
        log.error(f"Incorrect logging level passed! ({value})")
