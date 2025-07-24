#!/bin/sh
echo "wait_for_postgres.sh is running..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT"; do
  echo "$(date) - waiting for PostgreSQL..."
  sleep 2
done

echo "PostgreSQL is ready"

echo "Executing: $@"

exec "$@"