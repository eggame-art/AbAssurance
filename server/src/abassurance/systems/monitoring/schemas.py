from typing import Literal

from pydantic import BaseModel

ServiceId = Literal["oracle", "sqlserver", "kafka", "spark", "hdfs", "etl"]


class ServiceStatus(BaseModel):
    id: ServiceId
    name: str
    status: Literal["available", "unavailable", "not_checked"]
    message: str | None = None
