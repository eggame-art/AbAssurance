from collections.abc import Callable, Mapping
from typing import Any

Record = dict[str, Any]
Datasets = dict[str, list[Record]]
Mapper = Callable[[Mapping[str, Any], str], Record]
ValueConverter = Callable[[Any], Any]
TableMap = Mapping[str, tuple[str, str]]
