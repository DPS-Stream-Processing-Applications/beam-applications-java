FROM python:3.9-slim

WORKDIR /app

COPY . /app

COPY functions-service-pred.yaml /app/pred/functions-service.yaml
COPY functions-service-train.yaml /app/train/functions-service.yaml


RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "statefunStarter.py"]
#CMD ["python","test_creating_kubernetes.py"]