FROM python:3.10-slim

WORKDIR /server

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY main.py .
COPY log_store.py .
COPY config.json .

ENTRYPOINT ["python3", "main.py"]
