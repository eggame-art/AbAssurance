import logging
from unittest.mock import MagicMock

import pytest

from extraction.common import read_records
from shared.environment import Environment
from shared.errors import ConfigurationError, ExtractionError, error_context, run_cli


def test_whitespace_only_password_is_rejected() -> None:
    environment = Environment({"PASSWORD": "   "})

    with pytest.raises(ConfigurationError, match="Missing environment variable: PASSWORD"):
        environment.required("PASSWORD")


def test_password_whitespace_is_preserved() -> None:
    environment = Environment({"PASSWORD": "  test-secret  "})

    assert environment.required("PASSWORD") == "  test-secret  "


def test_boolean_configuration_accepts_surrounding_whitespace() -> None:
    environment = Environment({"ENABLED": " TRUE "})

    assert environment.boolean("ENABLED", "no") is True


def test_cursor_without_result_set_has_explicit_error() -> None:
    cursor = MagicMock(description=None)

    with pytest.raises(ExtractionError, match="Cursor does not contain a result set"):
        read_records(cursor)


def test_cli_error_retains_cause_without_logging_sensitive_details(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("test.shared.errors")
    original = ValueError("sensitive-password")

    def fail() -> None:
        with error_context("Connection failed", (ValueError,), ExtractionError):
            raise original

    with pytest.raises(ExtractionError) as captured:
        fail()

    assert captured.value.__cause__ is original

    with caplog.at_level(logging.ERROR):
        status = run_cli(fail, logger, "Pipeline failed")

    assert status == 1
    assert "Connection failed" in caplog.text
    assert "sensitive-password" not in caplog.text
