#!/bin/sh
set -e

host="$POSTGRES_HOST"
until pg_isready -h "$host" -p "$POSTGRES_PORT" -U "$POSTGRES_USER"; do
  echo "$(date) - waiting for PostgreSQL..."
  sleep 2
done

exec "$@"