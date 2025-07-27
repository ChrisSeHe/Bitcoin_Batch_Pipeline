#!/bin/sh

echo "wait_for_postgres.sh is running..."

until pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER"
do
  echo "$(date) - waiting for PostgreSQL..."
  sleep 2
done

exec "$@"