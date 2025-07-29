FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y postgresql-client && \
    apt-get clean

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["./wait_for_postgres.sh"]
CMD ["python", "aggregate.py"]