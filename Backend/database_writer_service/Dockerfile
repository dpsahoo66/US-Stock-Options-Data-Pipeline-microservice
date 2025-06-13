FROM python:3.9-slim

WORKDIR /app 
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt 
COPY database_writer_service . 
EXPOSE 8002 
CMD ["python", "manage.py", "runserver", "0.0.0.0:8002"]