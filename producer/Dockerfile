# Dockerfile for the producer application.
FROM python:2.7-alpine

# Install Python dependencies
RUN pip install pika

# Without this setting, Python never prints anything out.
ENV PYTHONUNBUFFERED=1

# Install the application
WORKDIR /usr/src/app

# It's only a single file.  It has to be in the same directory as the
# Dockerfile, or a subdirectory, but not a parent or sibling.
COPY producer.py .

# When you just 'docker run producer' with no command afterwards,
# default to this:
CMD ["python", "/usr/src/app/producer.py"]