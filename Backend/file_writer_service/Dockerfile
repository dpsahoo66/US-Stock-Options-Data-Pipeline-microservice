FROM python:3.9-slim

WORKDIR /app 
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt 
COPY file_writer_service . 
EXPOSE 8003 
CMD ["python", "manage.py", "runserver", "0.0.0.0:8003"]