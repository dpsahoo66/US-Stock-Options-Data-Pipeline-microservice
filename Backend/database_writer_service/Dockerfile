FROM python:3.9-slim

<<<<<<< Updated upstream
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
EXPOSE 8002
CMD ["python", "manage.py", "run_kafka_processor"]

=======
WORKDIR /app 
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt 
COPY database_writer_service . 
EXPOSE 8002 
CMD ["python", "manage.py", "runserver", "0.0.0.0:8002"]
>>>>>>> Stashed changes
