# Use official Python runtime as base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install required packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY dynamodb_export.py .

RUN mkdir /data


# Run script
CMD ["python", "dynamodb_export.py"]