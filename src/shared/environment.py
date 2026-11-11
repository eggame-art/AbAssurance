import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Self

from dotenv import dotenv_values

from shared.errors import ConfigurationError, ensure, error_context


@dataclass(frozen=True, slots=True)
class Environment:
    values: Mapping[str, str | None] = field(repr=False)

    @classmethod
    def from_file(cls, path: Path) -> Self:
        return cls({**dotenv_values(path), **os.environ})

    def get(self, name: str, default: str = "") -> str:
        return self.values.get(name) or default

    def required(self, name: str, fallback: str = "") -> str:
        result = self.get(name, fallback)

        ensure(bool(result.strip()), f"Missing environment variable: {name}", ConfigurationError)

        return result

    def integer(self, name: str, default: str, maximum: int) -> int:
        with error_context(f"{name} must be an integer", (ValueError,), ConfigurationError):
            result = int(self.get(name, default))

        ensure(1 <= result <= maximum, f"{name} must be between 1 and {maximum}", ConfigurationError)

        return result

    def boolean(self, name: str, default: str) -> bool:
        result = self.get(name, default).strip().lower()

        ensure(
            result in {"yes", "no", "true", "false", "1", "0"},
            f"{name} must be yes/no or true/false",
            ConfigurationError,
        )

        return result in {"yes", "true", "1"}
