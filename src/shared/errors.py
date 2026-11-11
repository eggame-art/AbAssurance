import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager


class ConfigurationError(ValueError):
    pass


class ExtractionError(RuntimeError):
    pass


class PipelineError(RuntimeError):
    pass


class InitializationError(RuntimeError):
    pass


APPLICATION_ERRORS = (ConfigurationError, ExtractionError, PipelineError, InitializationError, OSError)


def ensure(condition: bool, message: str, error_type: type[Exception]) -> None:
    if (not condition):
        raise error_type(message)


@contextmanager
def error_context(message: str, caught: tuple[type[Exception], ...], error_type: type[Exception]) -> Iterator[None]:
    try:
        yield

    except caught as exc:
        raise error_type(message) from exc


def run_cli(action: Callable[[], object], logger: logging.Logger, failure_message: str) -> int:
    try:
        action()

    except APPLICATION_ERRORS as exc:
        logger.error("%s: %s", failure_message, exc)

        return 1

    return 0
