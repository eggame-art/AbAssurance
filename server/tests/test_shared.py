import logging
from unittest.mock import MagicMock

import pytest
from abassurance.shared.environment import Environment
from abassurance.shared.errors import ConfigurationError, ExtractionError, error_context, run_cli
from abassurance.systems.data_integration.extraction.common import read_records


def test_whitespace_only_password_is_rejected() -> None:
    environment = Environment({"PASSWORD": "   "})

    with pytest.raises(ConfigurationError, match="Missing environment variable: PASSWORD"):
        environment.required("PASSWORD")


def test_password_whitespace_is_preserved() -> None:
    environment = Environment({"PASSWORD": "  test-secret  "})

    assert environment.required("PASSWORD") == "  test-secret  "


def test_secret_file_preserves_spaces_and_removes_final_newline(tmp_path):
    path = tmp_path / "password"
    path.write_text("  private-value  \n", encoding="utf-8")
    env = Environment({"PASSWORD_FILE": str(path)})

    assert env.secret("PASSWORD") == "  private-value  "
    assert "private-value" not in repr(env)


@pytest.mark.parametrize("content", [b"", b"  \n", b"x" * 4097, b"\xff"])
def test_invalid_secret_file_does_not_leak_content(tmp_path, content):
    path = tmp_path / "password"
    path.write_bytes(content)

    with pytest.raises(ConfigurationError) as captured:
        Environment({"PASSWORD_FILE": str(path)}).secret("PASSWORD")

    assert str(path) not in str(captured.value)
    assert "xxxx" not in str(captured.value)


def test_secret_file_fails_closed_without_environment_fallback(tmp_path):
    with pytest.raises(ConfigurationError, match="Cannot read secret: PASSWORD"):
        Environment({"PASSWORD_FILE": str(tmp_path / "missing")}).secret("PASSWORD", "fallback-value")
    with pytest.raises(ConfigurationError, match="not both"):
        Environment({"PASSWORD": "private-value", "PASSWORD_FILE": "missing"}).secret("PASSWORD")


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
