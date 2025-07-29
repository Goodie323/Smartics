# /backend/Dockerfile
FROM python:3.9
WORKDIR /app

# Copy requirements first
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy all backend files
COPY . .

# Run Flask
CMD ["python", "app/main.py"]