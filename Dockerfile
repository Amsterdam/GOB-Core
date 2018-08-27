FROM amsterdam/docker_python:latest
MAINTAINER datapunt@amsterdam.nl

# Install gobworkflow in /app folder
WORKDIR /app

# Copy testscript to where jenkins expect them
COPY test.sh /app/

# Install required Python packages
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Copy gobcore module
COPY gobcore gobcore
# Copy tests and config
COPY .flake8 .flake8
COPY tests tests
