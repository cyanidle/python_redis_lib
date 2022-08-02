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
    def __init__(self, *args: object) -> None:
        self.seconds = args[0]
        super().__init__(*args)

def async_oneshot_factory(*, logger:logging.Logger, on_shutdown:function = None):
    async def shutdownHook():
        if on_shutdown is None:
            pass
        else:
            coro = on_shutdown
            if coroutines.iscoroutine(coro):
                await coro
    def async_oneshot(func):
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
    return async_oneshot

def async_repeating_task_factory(logger:logging.Logger, delay, on_shutdown:function = None):
    async def shutdownHook():
        if on_shutdown is None:
            pass
        else:
            coro = on_shutdown
            if coroutines.iscoroutine(coro):
                await coro
    def async_repeating_task(func):
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
    return async_repeating_task