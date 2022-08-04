# -*- coding: utf-8 -*-
import logging
import logging.handlers
import os.path
from python_redis_lib.settings import LoggingSettings

def initLogging(caller_file:str, module_name:str, *,  settings: LoggingSettings = LoggingSettings()) -> logging.Logger:
    """
    Should be called with '__file__' as the first argument and 'module name' as in logging config as second!
    """
    log_root = logging.getLogger()
    log = logging.getLogger(module_name)
    if settings.add_timestamp:
        format = logging.Formatter('%(asctime)-15s %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s')
    else:
        format = logging.Formatter('%(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s')
    if settings.logfilename:
        logfilename = os.path.join(os.path.dirname(os.path.realpath(caller_file)), *(settings.logfilename.split("/")))
        file_handler = logging.handlers.RotatingFileHandler(
            logfilename, maxBytes = 524288000, backupCount = 2)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(format)
        log_root.addHandler(file_handler)
    if settings.enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(format)
        log_root.addHandler(console_handler)
    try:
        root_level_raw = settings.levels.pop("root")
    except KeyError:
        root_level_raw = None
        log.warn(f"Using default root logger level (INFO)!")
    try:
        if root_level_raw:
            root_level = _levels_dict[root_level_raw]
            log.setLevel(root_level)
    except KeyError:
        log.error(f"Incorrect logging level passed! ({root_level})")
    for name, val in settings.levels.items():
        _set_level(name, val, log)
    return log

_levels_dict = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR
}

def _set_level(name:str, value:str, log):
    try:
        logging.getLogger(name).setLevel(_levels_dict[value])
    except KeyError:
        log.error(f"Incorrect logging level passed! ({value})")
