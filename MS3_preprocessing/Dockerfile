FROM python:3.10

RUN apt-get update && apt-get install -y postgresql-client

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt
RUN chmod +x wait_for_postgres.sh

ENTRYPOINT ["./wait_for_postgres.sh"]
CMD ["python", "preprocess.py"]