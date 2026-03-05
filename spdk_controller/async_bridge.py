import asyncio
import threading
import logging
from concurrent.futures import Future
from typing import Any, Callable

logger = logging.getLogger(__name__)

class AsyncBridge:
    def __init__(self):
        self._loop: asyncio.AbstractEventLoop = None
        self._thread: threading.Thread = None
        self._running = False

    def start(self):
        if self._running:
            return
        self._thread = threading.Thread(
            target=self._loop_runner, 
            name="SPDK-Completion-Thread", 
            daemon=True
        )
        self._thread.start()
        # 等待 loop 初始化完成
        while self._loop is None or not self._loop.is_running():
            pass
        self._running = True
        logger.info("AsyncBridge: Completion event loop started.")

    def stop(self):
        if not self._running:
            return
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join()
        self._loop = None
        self._thread = None
        self._running = False
        logger.info("AsyncBridge: Stopped.")

    def _loop_runner(self):
        asyncio.set_event_loop(asyncio.new_event_loop())
        self._loop = asyncio.get_event_loop()
        self._loop.run_forever()
        self._loop.close()

    @property
    def loop(self):
        return self._loop

    def submit_blocking(self, spdk_func: Callable, *args) -> Any:
        """Call async C function and block until result."""
        if not self._running:
            raise RuntimeError("AsyncBridge is not running.")
        
        future = Future()
        
        def on_done(async_future):
            try:
                future.set_result(async_future.result())
            except Exception as e:
                future.set_exception(e)

        def do_submit():
            loop_future = self._loop.create_future()
            loop_future.add_done_callback(on_done)
            spdk_func(*args, loop_future)

        self._loop.call_soon_threadsafe(do_submit)
        return future.result(timeout=20)

    async def submit_async(self, spdk_func: Callable, *args) -> Any:
        """Awaitable wrapper for C function."""
        if not self._running:
            raise RuntimeError("AsyncBridge is not running.")
        loop_future = self._loop.create_future()
        self._loop.call_soon_threadsafe(spdk_func, *args, loop_future)
        return await loop_future

    def run_coroutine(self, coro):
        """Schedule a coroutine on the loop."""
        if self._running:
            self._loop.call_soon_threadsafe(lambda: self._loop.create_task(coro))

# Global Singleton
bridge = AsyncBridge()