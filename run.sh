#!/usr/bin/env bash
# Usage:
#   ./run.sh test                      run all tests
#   ./run.sh broker [spring args]      start the broker + REST API   (e.g. ./run.sh broker --minikafka.port=9093)
#   ./run.sh demo [host] [port]        two producers + a consumer group against a RUNNING broker, fills logs/audit.log
#   ./run.sh bench [key=value ...]     write/read benchmarks            (appends= threads= payload= records= reads= dir=)
#   ./run.sh cluster start|status|stop|clean    3-node cluster on this machine
set -e
cd "$(dirname "$0")"
MVN=mvn; [ -x ./mvnw ] && MVN=./mvnw
case "$1" in
  test)    $MVN -q test ;;
  broker)  shift; $MVN -q spring-boot:run -Dspring-boot.run.arguments="$*" ;;
  demo)    shift; $MVN -q compile; java -cp target/classes minikafka.cli.Demo "$@" ;;
  bench)   shift; $MVN -q compile; java -cp target/classes minikafka.bench.Bench "$@" ;;
  cluster) shift; MVN=$MVN ./scripts/cluster.sh "$@" ;;
  *) sed -n '2,8p' "$0" ;;
esac
