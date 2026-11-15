#!/usr/bin/env bash
# Runs a 3-node MiniKafka cluster on this machine, from the jar.
#
#   ./scripts/cluster.sh start     build the jar if needed and start node-1, node-2, node-3
#   ./scripts/cluster.sh status    who is leader (asks every node)
#   ./scripts/cluster.sh stop      stop all nodes           (or: kill one node to watch a failover)
#   ./scripts/cluster.sh clean     delete cluster/ (data, logs, audit files of the three nodes)
#
# Node i:  TCP broker port 9091+i   REST port 8080+i   files in cluster/node-i/{data,logs}
# Each node has its OWN audit log: cluster/node-i/logs/audit.log  (leader election, replication, who produced/consumed)
set -e
cd "$(dirname "$0")/.."
DIR=cluster
jar() { ls target/minikafka-*.jar 2>/dev/null | grep -v '\.original$' | head -1; }

peers_for() {                       # the other two nodes as id=host:port
  local me=$1 out=""
  for j in 1 2 3; do
    [ "$j" = "$me" ] && continue
    out="${out:+$out,}node-$j=localhost:$((9091 + j))"
  done
  echo "$out"
}

case "$1" in
  start)
    [ -n "$(jar)" ] || { echo "building the jar..."; ${MVN:-mvn} -q package -DskipTests; }
    mkdir -p "$DIR"
    for i in 1 2 3; do
      mkdir -p "$DIR/node-$i/logs"
      if [ -f "$DIR/node-$i/pid" ] && kill -0 "$(cat "$DIR/node-$i/pid")" 2>/dev/null; then echo "node-$i already running"; continue; fi
      nohup java -jar "$(jar)" \
        --server.port=$((8080 + i)) \
        --minikafka.port=$((9091 + i)) \
        --minikafka.node-id=node-$i \
        --minikafka.data-dir="$DIR/node-$i/data" \
        --minikafka.audit.file="$DIR/node-$i/logs/audit.log" \
        --logging.file.name="$DIR/node-$i/logs/app.log" \
        --minikafka.cluster.enabled=true \
        --minikafka.cluster.heartbeat-ms=200 \
        --minikafka.cluster.election-timeout-ms=1500 \
        --minikafka.cluster.peers="$(peers_for $i)" \
        > "$DIR/node-$i/console.out" 2>&1 &
      echo $! > "$DIR/node-$i/pid"
      echo "started node-$i  (tcp $((9091 + i)), rest $((8080 + i)), pid $(cat "$DIR/node-$i/pid"))"
    done
    echo "give it ~5 seconds to elect a leader, then:  ./scripts/cluster.sh status"
    ;;
  status)
    for i in 1 2 3; do
      printf "node-%s  " "$i"
      curl -s -m 2 "http://localhost:$((8080 + i))/api/cluster" | sed -E 's/.*"role":"([A-Z]+)","term":([0-9]+),"leaderId":("?[a-z0-9-]*"?).*/role=\1 term=\2 leader=\3/' || true
      echo
    done
    ;;
  stop)
    for i in 1 2 3; do
      if [ -f "$DIR/node-$i/pid" ]; then kill "$(cat "$DIR/node-$i/pid")" 2>/dev/null && echo "stopped node-$i" || true; rm -f "$DIR/node-$i/pid"; fi
    done
    ;;
  clean) rm -rf "$DIR" ;;
  *) echo "usage: $0 start|status|stop|clean" ;;
esac
