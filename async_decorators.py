# -*- coding: utf-8 -*-
from asyncio import coroutines
import functools
import logging
import traceback
import asyncio

class ContinueLoop(Exception):
    pass
class ReturnFromLoop(Exception):
    pass
class LoopSleep(Exception):
    def __init__(self, seconds:float, *args: object) -> None:
        self.seconds = seconds
        super().__init__(*args)

def async_oneshot(* , logger:logging.Logger, on_shutdown = None):
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
    return _async_oneshot

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
        async def oneshot_wrapper(*args, **kwargs):
            try:
                while True:
                    try:
                        await func(*args, **kwargs)
                        await asyncio.sleep(delay)
                    except ContinueLoop:
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
        return oneshot_wrapper
    return _async_repeating_task