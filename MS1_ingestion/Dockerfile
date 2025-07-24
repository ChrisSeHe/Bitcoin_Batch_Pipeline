FROM python:3.10

RUN apt-get update && apt-get install -y postgresql-client

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app

COPY wait_for_postgres.sh /wait_for_postgres.sh
RUN chmod +x /wait_for_postgres.sh

ENTRYPOINT ["/wait_for_postgres.sh"]
CMD ["python", "ingest.py"]