FROM python:3.11-slim-buster

# immediately write stdout and stderr
ENV PYTHONUNBUFFERED=1

# install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# create a directory and cd into it
WORKDIR /app

COPY /src/* .

# this command can be overwritten when running the container
CMD ["pytest", "tests.py"]