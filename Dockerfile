FROM amsterdam/python:3.7-buster
MAINTAINER datapunt@amsterdam.nl

# Install gobworkflow in /app folder
WORKDIR /app

# Copy testscript to where jenkins expect them
COPY test.sh /app/

# Install required Python packages
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt
RUN mkdir -m 777 -p /root/gob-volume/message_broker

# Copy gobcore module
COPY gobcore gobcore
# Copy tests and config
COPY .flake8 .flake8
COPY tests tests
