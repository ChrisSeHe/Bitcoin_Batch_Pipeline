FROM python:3.10

WORKDIR /app

COPY . .
COPY wait_for_postgres.sh .

RUN apt-get update && apt-get install -y postgresql-client

RUN chmod +x wait_for_postgres.sh

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["./wait_for_postgres.sh"]
CMD ["python", "upload_to_minio.py"]