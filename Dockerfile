# Use an official Python runtime as a parent image
FROM python:3.9-slim-buster

# Set the working directory in the container
WORKDIR /app

# Install system dependencies required for headless Chrome and webdriver-manager
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    # Dependencies for Chrome
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libcups2 \
    libasound2 \
    libpango-1.0-0 \
    # Dependencies for running Chrome headless (some might be redundant but good to have)
    ca-certificates \
    fonts-liberation \
    lsb-release \
    xdg-utils \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Download and install Google Chrome (headless)
# You can check for the latest stable version if needed
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y \
    google-chrome-stable \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
# Using --no-cache-dir to reduce image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script into the container at /app
COPY CMESOFR.py .
COPY CNBC_US10YTIP_History.py .

# Make Python output unbuffered, which is useful for logging in Fargate/CloudWatch
ENV PYTHONUNBUFFERED 1

# Set the default command to execute when the container starts
# This will run your Python script.
# The DYNAMODB_TABLE environment variable will be set in Fargate Task Definition
CMD ["python", "CMESOFR.py"]
