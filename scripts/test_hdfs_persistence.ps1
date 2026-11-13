$ErrorActionPreference = 'Stop'
$persistenceRoot = 'hdfs://namenode:9000/abassurance-tests/persistence-' + [guid]::NewGuid().ToString('N')

docker compose --profile hdfs up -d --wait namenode datanode
if ($LASTEXITCODE -ne 0) { throw 'HDFS startup failed' }

docker compose run --rm --no-deps -e "HDFS_PERSISTENCE_ROOT=$persistenceRoot" -e HDFS_PERSISTENCE_PHASE=write spark-hdfs python -m pytest -q tests/spark/test_hdfs.py --run-hdfs-integration -k persistence
if ($LASTEXITCODE -ne 0) { throw 'HDFS write verification failed' }

docker compose restart namenode datanode
if ($LASTEXITCODE -ne 0) { throw 'HDFS restart failed' }

docker compose --profile hdfs up -d --wait namenode datanode
if ($LASTEXITCODE -ne 0) { throw 'HDFS did not become healthy after restart' }

docker compose run --rm --no-deps -e "HDFS_PERSISTENCE_ROOT=$persistenceRoot" -e HDFS_PERSISTENCE_PHASE=read spark-hdfs python -m pytest -q tests/spark/test_hdfs.py --run-hdfs-integration -k persistence
if ($LASTEXITCODE -ne 0) { throw 'HDFS persistence verification failed' }

Write-Output "Persistence verified: $persistenceRoot"
