# syntax=docker/dockerfile:1
FROM amsterdam/gob_wheelhouse:3.9-buster as wheelhouse

# Application stage.
FROM amsterdam/gob_baseimage:3.9-buster as application

# Fill the wheelhouse.
COPY --from=wheelhouse /opt/wheelhouse /opt/wheelhouse

# Install gobcore in /app folder.
WORKDIR /app

# Install required Python packages.
COPY requirements.txt /app/
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install --no-cache-dir \
	--find-links /opt/wheelhouse --requirement requirements.txt
RUN rm requirements.txt

# Wheelhouse cleanup.
RUN rm -rf /opt/wheelhouse

RUN mkdir -m 2755 -p /home/datapunt/gob-volume/message_broker && chown datapunt.datapunt /home/datapunt/gob-volume/message_broker
# Airflow (standalone).
RUN mkdir -m 2755 /airflow && chown datapunt.datapunt /airflow

# Copy gobcore module.
COPY gobcore gobcore

# Copy test module and tests to where Jenkins expects them.
COPY test.sh pyproject.toml ./
COPY tests tests

USER datapunt
