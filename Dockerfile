FROM adoptopenjdk/maven-openjdk8:latest
# we need maven < 3.8 so siddhi can be built

ENV HOME=/root/home
ENV TZ=Europe/Berlin 
WORKDIR $HOME

RUN apt update && DEBIAN_FRONTEND="noninteractive" apt install -y python3 python3-dev \
python3-pip libboost-python-dev libboost-all-dev build-essential g++ autotools-dev \
libicu-dev libbz2-dev wget unzip git zlib1g zlib1g-dev libssl-dev libbz2-dev \
libsqlite3-dev libffi-dev build-essential python-dev python-setuptools python-pip \
python-smbus libncursesw5-dev libgdbm-dev libc6-dev tk-dev

RUN wget https://github.com/siddhi-io/siddhi-sdk/releases/download/v5.1.0/siddhi-sdk-5.1.0.zip
RUN wget https://github.com/siddhi-io/PySiddhi/releases/download/v5.1.0/siddhi-python-api-proxy-5.1.0.jar
RUN unzip siddhi-sdk-5.1.0.zip && rm siddhi-sdk-5.1.0.zip
ENV SIDDHISDK_HOME=$HOME/siddhi-sdk-5.1.0
RUN mv siddhi-python-api-proxy-5.1.0.jar $SIDDHISDK_HOME/lib

# immediately write python output to stdout/stderr
ENV PYTHONUNBUFFERED=1


# install PySiddhi
ENV CPLUS_INCLUDE_PATH=/usr/include/python3.6m:$CPLUS_INCLUDE_PATH
RUN pip3 install cython
RUN pip3 install pysiddhi@git+https://github.com/siddhi-io/PySiddhi.git@v5.1.0

COPY requirements.txt .
RUN pip3 install -r requirements.txt

# create a directory and cd into it
WORKDIR /app
ENV PYTHONPATH=$PYTHONPATH:/app

COPY /src/ ./

# this command can be overwritten when running the container
CMD  ["pytest", "tests", "-s", "-vv"]