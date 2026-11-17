#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  namenode)
    mkdir -p /hadoop/dfs/name

    if [[ ! -f /hadoop/dfs/name/current/VERSION ]]; then
      if [[ -n "$(ls -A /hadoop/dfs/name)" ]]; then
        echo "NameNode directory is not empty and has no VERSION; refusing to format" >&2
        exit 1
      fi

      hdfs namenode -format -nonInteractive
    fi

    exec hdfs namenode
    ;;
  datanode)
    mkdir -p /hadoop/dfs/data
    exec hdfs datanode
    ;;
  *)
    echo "Expected namenode or datanode" >&2
    exit 1
    ;;
esac
