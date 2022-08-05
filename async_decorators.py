# -*- coding: utf-8 -*-
from asyncio import coroutines
import functools
import logging
import traceback
import asyncio

log = logging.getLogger(__name__)

class ReapeatingControlExceptions(Exception):
    """(async_repeating_task) Base class for repeating task control Exceptions"""
    def __init__(self, *args: object) -> None:
        """(async_repeating_task) Base class for repeating task control Exceptions"""
        super().__init__(*args)

class LoopContinue(ReapeatingControlExceptions):
    """(async_repeating_task) Raise to restart task soon"""
    def __init__(self, *args: object) -> None:
        """(async_repeating_task) Raise to restart task soon"""
        super().__init__(*args)
    
class LoopReturn(ReapeatingControlExceptions):
    """(async_repeating_task) Raise to finish task"""
    def __init__(self, *args: object) -> None:
        """(async_repeating_task) Raise to finish task"""
        super().__init__(*args)

class LoopSleep(ReapeatingControlExceptions):
    """(async_repeating_task) Raise to restart task after delay"""
    def __init__(self, seconds:float, *args: object) -> None:
        """(async_repeating_task) Raise to restart task after delay"""
        self.seconds = seconds
        super().__init__(*args)

def async_oneshot(func = None, **kwargs):
    logger:logging.Logger = kwargs.get("logger") or log
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
            except Exception as e:
                logger.error("Unexpected error occured:")
                logger.error(traceback.format_exc())
                if isinstance(e, ReapeatingControlExceptions):
                    logger.error(f"Exceptions for controlling @async_repeating_task should not be passed to @async_oneshot")
                else:                    
                    raise e
        return oneshot_wrapper
    if kwargs:
        return _async_oneshot
    return _async_oneshot(func)

def async_repeating_task(*, delay:float, on_shutdown = None, logger = log):
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
                    except LoopContinue:
                        await asyncio.sleep(0.01)
                        continue
                    except LoopReturn as e:
                        if e.args:
                            logger.warn(f"Canceling repeating task. Reason: {e.args[0] or 'Not Given'}") 
                        return
                    except LoopSleep as sleep:
                        await asyncio.sleep(sleep.seconds)
                        continue
                    except Exception as e:
                        if isinstance(e, ReapeatingControlExceptions):
                            logger.error(f"Repeating Control Exception not handled correctly!")
                        raise
            except KeyboardInterrupt:
                logger.info('Ctrl-C process shutdown requested. Closing...')
                await shutdownHook()
            except asyncio.exceptions.CancelledError:
                raise
            except:
                logger.error("Unexpected error occured:")
                logger.error(traceback.format_exc())
                raise
        return repeating_wrapper
    return _async_repeating_task

def async_handle_exceptions(handler):
    """
    Handler is a function, that accepts a coroutine and its *args, **kwargs
    --\n
    This is supposed to be used in tandem with @async_repeating_task or @async_oneshot
    --\n
    @async_repeating_task(delay = 1)\n
    @async_handle_exceptions(my_handler)\n
    async def my_task(self, *args):
        await do_stuff_once_per_second()
    ----------------------------\n
    Handler should look like:
    --\n
    async def handler(coro):
        try:
            if my_condition():
                return await coro    <--- Dont forget 'return'
            else:
                raise LoopSleep(1)
        except MyException as e:
            handle_my_error(e)
    """
    def _async_handle_exceptions(func):
        @functools.wraps(func)
        async def _exception_handle_impl(*args, **kwargs):
            try:
                return await handler(func(*args, **kwargs), *args, **kwargs)
            except:
                raise
        return _exception_handle_impl
    return _async_handle_exceptions