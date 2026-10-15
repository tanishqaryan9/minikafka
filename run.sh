#!/usr/bin/env bash
# Usage: ./run.sh test   |   ./run.sh broker [extra spring args, e.g. --minikafka.port=9093]
set -e
cd "$(dirname "$0")"
MVN=mvn; [ -x ./mvnw ] && MVN=./mvnw
case "$1" in
  test)   $MVN -q test ;;
  broker) shift; $MVN -q spring-boot:run -Dspring-boot.run.arguments="$*" ;;
  *) echo "usage: ./run.sh test | broker [spring args]" ;;
esac
