FROM python:3.11

#WORKDIR /app

COPY requirements.txt .
COPY src/main .
RUN pip install --no-cache-dir -r requirements.txt

#COPY src/main /app

#CMD ["sh", "-c","ls /home"]
CMD ["python", "main.py"]
