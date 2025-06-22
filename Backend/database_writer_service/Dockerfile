FROM python:3.9-slim

WORKDIR /app 
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt 
COPY . .
EXPOSE 8002
CMD ["python", "manage.py", "run_kafka_writer"]


# FROM python:3.9-slim

# # Install system dependencies for pyodbc and build tools
# RUN apt-get update && apt-get install -y \
#     build-essential \
#     unixodbc-dev \
#     python3-dev \
#     && rm -rf /var/lib/apt/lists/*

# WORKDIR /app

# # Copy requirements and install Python packages
# COPY requirements.txt .

# RUN pip install --upgrade pip
# RUN pip install --no-cache-dir -r requirements.txt

# # Copy your application code
# COPY . .

# EXPOSE 8002

# CMD ["python", "manage.py", "run_kafka_writer"]
