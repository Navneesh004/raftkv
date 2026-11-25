#!/usr/bin/env bash
# Chaos test: run the cluster in Docker, keep a checking client writing, and
# kill -9 a random replica every few seconds, restarting it a little later.
#
#   ./deploy/chaos.sh [seconds] [clients]
#   COMPOSE_FILE=deploy/docker-compose.5.yml ./deploy/chaos.sh 600
#
# `docker kill` sends SIGKILL: the process dies instantly with no cleanup,
# exactly like a crash. The replica then restarts from its on-disk state.
# Exits non-zero if the load generator saw a single wrong read.
set -euo pipefail

DURATION=${1:-300}
CLIENTS=${2:-8}
export COMPOSE_FILE=${COMPOSE_FILE:-deploy/docker-compose.yml}
MAX_DOWN=${MAX_DOWN:-1} # replicas that may be down at once (1 for 3 nodes, 2 for 5)

cd "$(dirname "$0")/.."
dc() { docker compose "$@"; }

echo ">> building and starting cluster ($COMPOSE_FILE)"
dc up -d --build
NODES=($(dc config --services | grep '^node' | sort))
sleep 3
dc run --rm -T cli status || true

echo ">> starting load generator for ${DURATION}s with ${CLIENTS} clients"
dc run --rm -T loadgen -clients "$CLIENTS" -duration "${DURATION}s" &
LOADGEN=$!

kills=0
down=()
end=$((SECONDS + DURATION))
while [ $SECONDS -lt $end ] && kill -0 $LOADGEN 2>/dev/null; do
  sleep $((2 + RANDOM % 4))
  if [ ${#down[@]} -lt "$MAX_DOWN" ] && [ $((RANDOM % 3)) -ne 0 ]; then
    victim=${NODES[$((RANDOM % ${#NODES[@]}))]}
    if [[ ! " ${down[*]-} " =~ " $victim " ]]; then
      echo ">> [t=$SECONDS] kill -9 $victim"
      dc kill -s SIGKILL "$victim" >/dev/null
      down+=("$victim")
      kills=$((kills + 1))
    fi
  elif [ ${#down[@]} -gt 0 ]; then
    victim=${down[0]}
    echo ">> [t=$SECONDS] restart $victim"
    dc start "$victim" >/dev/null
    down=("${down[@]:1}")
  fi
done

for v in "${down[@]-}"; do
  [ -n "$v" ] && dc start "$v" >/dev/null
done

status=0
wait $LOADGEN || status=$?
echo ">> chaos finished: $kills kills; load generator exit status $status"
dc run --rm -T cli status || true
exit $status
