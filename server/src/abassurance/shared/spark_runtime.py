import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

T = TypeVar("T")


class SparkBusy(RuntimeError):
    pass


class SparkRuntime:
    def __init__(self, max_pending: int = 3):
        self.spark: SparkSession | None = None
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="api-spark")
        self.capacity = BoundedSemaphore(max_pending)

    def _call(self, action: Callable[["SparkSession"], T]) -> T:
        if self.spark is None:
            from abassurance.systems.spark_pipeline.run import create_session

            self.spark = create_session(kafka=False)
            self.spark.sparkContext.setLogLevel("WARN")
        return action(self.spark)

    async def run(self, action: Callable[["SparkSession"], T], timeout: float = 45) -> T:
        if not self.capacity.acquire(blocking=False):
            raise SparkBusy("Spark request queue is full")
        try:
            future = self.executor.submit(self._call, action)
        except BaseException:
            self.capacity.release()
            raise

        future.add_done_callback(lambda _: self.capacity.release())
        return await asyncio.wait_for(asyncio.wrap_future(future), timeout)

    def close(self) -> None:
        def stop():
            if self.spark is not None:
                self.spark.stop()
                self.spark = None

        try:
            self.executor.submit(stop).result()
        finally:
            self.executor.shutdown(wait=True, cancel_futures=True)
