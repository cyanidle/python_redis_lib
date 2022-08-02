# -*- coding: utf-8 -*-
import logging
import logging.handlers
import os.path

log = logging.getLogger()

def initLogging(caller_file:str, *, enable_console = True,logfilename: str = "", level = logging.INFO, add_timestamp = False):
    """
    Should be called with '__file__' as the first argument!
    """
    log.setLevel(level)
    if add_timestamp:
        format = logging.Formatter('%(asctime)-15s %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s')
    else:
        format = logging.Formatter('%(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s')
    if logfilename:
        logfilename = os.path.join(os.path.dirname(os.path.realpath(caller_file)), *(logfilename.split("/")))
        file_handler = logging.handlers.RotatingFileHandler(
            logfilename, maxBytes = 524288000, backupCount = 2)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(format)
        log.addHandler(file_handler)
    if enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(format)
        log.addHandler(console_handler)
    logging.getLogger('asyncio').setLevel(level)
    logging.getLogger('python_redis_lib.redis').setLevel(level)
    logging.getLogger('python_redis_lib.settings').setLevel(level)
    logging.getLogger('python_redis_lib.supervisor').setLevel(level)