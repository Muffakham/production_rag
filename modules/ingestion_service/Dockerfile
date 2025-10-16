# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Upgrade pip
RUN pip install --upgrade pip

# This is the crucial step: Create the package structure inside the container
# and copy the application code into the correct sub-directory.
COPY . /app/modules/ingestion_service/

# Create the __init__.py file in the parent `modules` directory to make it a package
RUN touch /app/modules/__init__.py

# Install the Python dependencies
RUN pip install --no-cache-dir -r /app/modules/ingestion_service/requirements.txt

# Command to run the application as a module from the WORKDIR (/app).
# This tells Python to look for a package named `modules`,
# then a sub-package `ingestion_service`, and run its `main` module.
CMD ["python", "-m", "modules.ingestion_service.main"]
