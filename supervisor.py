# -*- coding: utf-8 -*-
import asyncio
import logging
import logging.handlers
import signal
import sys
import time
import traceback
log = logging.getLogger()

class Supervisor:
    """
    Accepts any amount of supervised objects. 
    
    Objects should have: 
    
        run() method for restart in case of error
        
        handleTerm() for shutdown sequences. 
        
        Both are optional.

    Needs used 'asyncio.EventLoop' object passed as 'ioloop' argument
    """
    def __init__(self, *args:object, ioloop: asyncio.AbstractEventLoop) -> None:
        self.ioloop = ioloop
        self.obj_list = list(args)
        for obj in self.obj_list:
            log.info(f"Adding {obj} to supervisor tracking")
        self.terminated = False
        ioloop.set_exception_handler(self.errorHook)
        ioloop.add_signal_handler(signal.SIGTERM, self.handleTerm)
        ioloop.add_signal_handler(signal.SIGINT, self.handleTerm)
    def handleTerm(self):
        self.terminated = True
        for task in asyncio.all_tasks():
            task.cancel()
        loop = asyncio.get_running_loop()
        for obj in self.obj_list:
            if hasattr(obj,"handleTerm") and callable(getattr(obj, "handleTerm")):
                log.warn(f"Starting shutdown sequence for {obj}")
                coro = obj.handleTerm()
                if asyncio.coroutines.iscoroutine(coro):
                    loop.create_task(coro)
            else:
                log.warn(f"Supervised object {obj} does not have a 'handleTerm()' method!")
        loop.create_task(self._shutdownChecker())
    async def _shutdownChecker(self):
        await asyncio.sleep(3)
        while True:
            if len(asyncio.all_tasks()) == 1:
                sys.exit(0)
            await asyncio.sleep(0)
    def errorHook(self, loop:asyncio.AbstractEventLoop, context = None):
        if self.terminated:
            return
        if not context:
            context = {"message":"No context given"}
        msg = context.get("exception", context["message"])
        point_of_error = context.get("future")
        if not point_of_error is None:
            point_of_error = point_of_error._coro
        else:
            "Unknown"
        log.error(f"Caught exception: {msg}. Point of error {point_of_error}")
        for task in asyncio.all_tasks():
            task.cancel()
        time.sleep(5)
        self._rerunAll()
    def registerNew(self, obj):
        self.obj_list.append(obj)
        log.info(f"Registered new object in supervising list '{obj}'")
    def remove(self,obj):
        log.info(f"Removing object '{obj}' from supervising list")
        self.obj_list.remove(obj)
    def runAll(self):
        for obj in self.obj_list:
            try:
                if hasattr(obj,"run") and callable(getattr(obj, "run")):
                    log.info(f"Starting {obj}...")
                    obj.run()
                else:
                    log.warn(f"Object {obj} does not have a run() method to be initialised!")
            except:
                log.error(f"Could not start supervisor client {obj}, full reason:")
                log.error(traceback.format_exc())
    def _rerunAll(self):
        for obj in self.obj_list:
            try:
                if hasattr(obj,"run") and callable(getattr(obj, "run")):
                    log.info(f"Restarting {obj}...")
                    obj.run()
                else:
                    log.warn(f"Object {obj} does not have a run() method to be restarted on error!")
            except:
                log.error(f"Could not restart supervisor client {obj}, full reason:")
                log.error(traceback.format_exc())