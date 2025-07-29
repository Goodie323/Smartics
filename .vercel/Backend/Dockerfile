FROM python:3.9
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy entire app
COPY ./app ./app

# Expose port and run
EXPOSE 5000
CMD ["python", "app/main.py"]