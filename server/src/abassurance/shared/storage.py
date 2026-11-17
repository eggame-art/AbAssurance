import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from abassurance.shared.errors import ConfigurationError


@contextmanager
def state_lock(state: Path) -> Iterator[None]:
    state.mkdir(parents=True, exist_ok=True)

    with (state / ".lock").open("a+b") as handle:
        handle.seek(0, os.SEEK_END)

        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()

        handle.seek(0)

        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise ConfigurationError("State directory is already locked") from exc

        try:
            yield
        finally:
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def write_json_atomic(path: Path, value: dict) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")

    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
