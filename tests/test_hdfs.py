from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from messaging.config import KafkaConfig
from shared.errors import ConfigurationError
from spark_pipeline.hdfs import HdfsState, read_snapshot, validate_hdfs_root
from spark_pipeline.run import run_available


@pytest.mark.parametrize(
    "root",
    [
        "/local",
        "file:///tmp/state",
        "s3://bucket/state",
        "hdfs:///state",
        "hdfs://namenode/",
        "hdfs://namenode/a/../b",
        "hdfs://namenode/a//b",
        "hdfs://user@namenode/state",
        "hdfs://namenode/state?x=1",
        "hdfs://namenode/state#x",
        "hdfs://namenode:bad/state",
        "hdfs://namenode:0/state",
        "hdfs://namenode/a\\b",
    ],
)
def test_hdfs_rejects_ambiguous_or_local_storage_paths(root: str) -> None:
    with pytest.raises(ConfigurationError):
        validate_hdfs_root(root)


def test_hdfs_uri_is_preserved_without_local_path_resolution() -> None:
    assert validate_hdfs_root("hdfs://namenode:9000/abassurance/") == "hdfs://namenode:9000/abassurance"


def test_unsupported_uri_never_becomes_local_path() -> None:
    with pytest.raises(ConfigurationError, match="Unsupported"):
        run_available(MagicMock(), KafkaConfig(), "s3://bucket/state")


@pytest.mark.parametrize("attribute", ["_jvm", "_gateway", "_jsc"])
def test_hdfs_requires_an_active_spark_jvm_context(attribute: str) -> None:
    spark = MagicMock()
    setattr(spark.sparkContext, attribute, None)

    with pytest.raises(ConfigurationError, match="active Spark JVM context"):
        HdfsState(spark, "hdfs://namenode:9000/test")

    if attribute != "_jsc":
        spark.sparkContext._jsc.hadoopConfiguration.assert_not_called()


def test_hdfs_manifest_is_closed_before_atomic_overwrite() -> None:
    state = HdfsState(MagicMock(), "hdfs://namenode:9000/test")
    events = MagicMock()
    stream = state.fs.create.return_value
    events.attach_mock(stream.hsync, "sync")
    events.attach_mock(stream.close, "close")
    events.attach_mock(state.context.rename, "rename")

    state.write_json_atomic("CURRENT.json", {"snapshot": "complete"})

    assert [call[0] for call in events.mock_calls] == ["sync", "close", "rename"]
    assert state.gateway.new_array.return_value.__setitem__.call_args.args[1] == (
        state.jvm.org.apache.hadoop.fs.Options.Rename.OVERWRITE
    )
    state.fs.create.assert_called_once()
    assert state.fs.create.call_args.args[1] is False


def test_failed_hdfs_manifest_write_does_not_publish() -> None:
    state = HdfsState(MagicMock(), "hdfs://namenode:9000/test")
    state.fs.create.return_value.hsync.side_effect = OSError("Disk failure")

    with pytest.raises(OSError, match="Disk failure"):
        state.write_json_atomic("CURRENT.json", {"snapshot": "incomplete"})

    state.context.rename.assert_not_called()
    state.fs.create.return_value.close.assert_called_once()
    state.fs.delete.assert_called_once()


def test_hdfs_exclusive_lock_rejects_second_writer() -> None:
    state = HdfsState(MagicMock(), "hdfs://namenode:9000/test")
    state.fs.create.side_effect = OSError("File exists")
    state.fs.exists.return_value = True

    with pytest.raises(ConfigurationError, match="already in use"), state.lock():
        pytest.fail("Acquired an existing lock")

    state.fs.delete.assert_not_called()


def test_hdfs_lock_releases_after_failed_processing() -> None:
    state = HdfsState(MagicMock(), "hdfs://namenode:9000/test")

    with pytest.raises(RuntimeError, match="Processing failed"), state.lock():
        raise RuntimeError("Processing failed")

    state.fs.delete.assert_called_once()


def test_hdfs_binding_keeps_raw_and_checkpoint_together() -> None:
    state = HdfsState(MagicMock(), "hdfs://namenode:9000/test")
    config = KafkaConfig()
    state.read_json = MagicMock(
        return_value={"version": 1, "bootstrap_servers": config.bootstrap_servers, "topics": config.topics()}
    )
    state.exists = MagicMock(side_effect=lambda path: path in ("source.json", "raw"))

    with pytest.raises(ConfigurationError, match="together"):
        state.bind(config)

    with pytest.raises(ConfigurationError, match="source changed"):
        state.bind(replace(config, topic_prefix="other"))


def test_reader_validates_entity_before_connecting() -> None:
    spark = MagicMock()

    with pytest.raises(ConfigurationError, match="Unknown"):
        read_snapshot(spark, "hdfs://namenode:9000/test", "unknown")

    assert spark.mock_calls == []
