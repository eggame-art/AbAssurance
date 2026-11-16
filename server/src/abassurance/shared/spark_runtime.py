import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

T = TypeVar("T")


class SparkRuntime:
    def __init__(self):
        self.spark: SparkSession | None = None
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="api-spark")

    def _call(self, action: Callable[["SparkSession"], T]) -> T:
        if self.spark is None:
            from abassurance.systems.spark_pipeline.run import create_session

            self.spark = create_session(kafka=False)
            self.spark.sparkContext.setLogLevel("WARN")
        return action(self.spark)

    async def run(self, action: Callable[["SparkSession"], T], timeout: int = 45) -> T:
        future = self.executor.submit(self._call, action)
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
