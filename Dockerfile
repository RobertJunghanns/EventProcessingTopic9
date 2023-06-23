FROM python:3.11-slim-buster

# immediately write stdout and stderr
ENV PYTHONUNBUFFERED=1

# create a directory and cd into it
WORKDIR /app

# we use the same Dockerfile for client and server
COPY . .

# this command is overwritten in docker-compose
# depending on whether the container should serve as client or server
CMD ["python", "main.py"]