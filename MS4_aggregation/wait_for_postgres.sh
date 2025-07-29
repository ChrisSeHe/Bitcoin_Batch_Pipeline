#!/bin/bash

echo "Waiting for PostgreSQL to be available..."

RETRIES=30

until pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" || [ $RETRIES -eq 0 ]; do
  echo "Postgres is unavailable - sleeping"
  sleep 2
  ((RETRIES--))
done

if [ $RETRIES -eq 0 ]; then
  echo "Postgres not available after waiting, exiting."
  exit 1
fi

echo "Postgres is available - continuing..."
exec "$@"