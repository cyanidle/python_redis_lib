# -*- coding: utf-8 -*-
from asyncio import coroutines
import functools
import logging
import traceback
import asyncio

class ContinueLoop(Exception):
    """(async_repeating_task) Raise to restart task soon"""
    def __init__(self, *args: object) -> None:
        """(async_repeating_task) Raise to restart task soon"""
        super().__init__(*args)
    
class ReturnFromLoop(Exception):
    """(async_repeating_task) Raise to finish task"""
    def __init__(self, *args: object) -> None:
        """(async_repeating_task) Raise to finish task"""
        super().__init__(*args)

class LoopSleep(Exception):
    """(async_repeating_task) Raise to restart task after delay"""
    def __init__(self, seconds:float, *args: object) -> None:
        """(async_repeating_task) Raise to restart task after delay"""
        self.seconds = seconds
        super().__init__(*args)

def async_oneshot(func = None, **kwargs):
    logger:logging.Logger = kwargs.get("logger") or logging.getLogger() 
    on_shutdown = kwargs.get("on_shutdown")
    def _async_oneshot(func):
        async def shutdownHook():
            if on_shutdown is None:
                pass
            else:
                coro = on_shutdown
                if coroutines.iscoroutine(coro):
                    await coro
        @functools.wraps(func)
        async def oneshot_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except KeyboardInterrupt:
                logger.info('Ctrl-C process shutdown requested. Closing...')
                await shutdownHook()
            except asyncio.exceptions.CancelledError:
                raise
            except:
                logger.error("Error occured:")
                logger.error(traceback.format_exc())
                raise
        return oneshot_wrapper
    if kwargs:
        return _async_oneshot
    return _async_oneshot(func)

def async_repeating_task(*, delay:float, on_shutdown = None, logger = logging.getLogger()):
    def _async_repeating_task(func):
        async def shutdownHook():
            if on_shutdown is None:
                pass
            else:
                coro = on_shutdown
                if coroutines.iscoroutine(coro):
                    await coro
        @functools.wraps(func)
        async def repeating_wrapper(*args, **kwargs):
            try:
                while True:
                    try:
                        await func(*args, **kwargs)
                        await asyncio.sleep(delay)
                    except ContinueLoop:
                        await asyncio.sleep(0.01)
                        continue
                    except ReturnFromLoop:
                        return
                    except LoopSleep as sleep:
                        await asyncio.sleep(sleep.seconds)
                        continue
                    except:
                        raise
            except KeyboardInterrupt:
                logger.info('Ctrl-C process shutdown requested. Closing...')
                await shutdownHook()
            except asyncio.exceptions.CancelledError:
                raise
            except:
                logger.error("Error occured:")
                logger.error(traceback.format_exc())
                raise
        return repeating_wrapper
    return _async_repeating_task