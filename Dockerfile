FROM amsterdam/python:3.9-buster
MAINTAINER datapunt@amsterdam.nl

# Install GDAL and ODBC
RUN apt-get update
RUN apt-get install -y --no-install-recommends libgdal-dev unixodbc-dev

# Update C env vars so compiler can find gdal
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

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
