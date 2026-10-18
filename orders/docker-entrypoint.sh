#!/bin/sh
set -e

attempt=0
until pgmigrate -d /pgmigrate -t latest -c "$DATABASE_URL" migrate; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 30 ]; then
        echo "pgmigrate failed after $attempt attempts" >&2
        exit 1
    fi
    sleep 1
done

exec /orders
