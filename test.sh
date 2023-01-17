#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

export COVERAGE_FILE="/tmp/.coverage"

FILES=(
#  gobcore/__init__.py
#  gobcore/datastore/__init__.py
#  gobcore/datastore/bag_extract.py
#  gobcore/datastore/datastore.py
#  gobcore/datastore/factory.py
#  gobcore/datastore/file.py
#  gobcore/datastore/objectstore.py
#  gobcore/datastore/oracle.py
#  gobcore/datastore/postgres.py
#  gobcore/datastore/sftp.py
#  gobcore/datastore/sql.py
#  gobcore/datastore/sqlserver.py
#  gobcore/datastore/wfs.py
  gobcore/enum.py
#  gobcore/events/__init__.py
#  gobcore/events/import_events.py
#  gobcore/events/import_message.py
#  gobcore/exceptions.py
#  gobcore/logging/__init__.py
#  gobcore/logging/audit_logger.py
#  gobcore/logging/log_publisher.py
#  gobcore/logging/logger.py
#  gobcore/message_broker/__init__.py
#  gobcore/message_broker/async_message_broker.py
#  gobcore/message_broker/config.py
#  gobcore/message_broker/events.py
#  gobcore/message_broker/initialise_queues.py
#  gobcore/message_broker/message_broker.py
#  gobcore/message_broker/messagedriven_service.py
#  gobcore/message_broker/notifications.py
#  gobcore/message_broker/offline_contents.py
#  gobcore/message_broker/typing.py
#  gobcore/message_broker/utils.py
#  gobcore/model/__init__.py
#  gobcore/model/amschema/__init__.py
#  gobcore/model/amschema/model.py
#  gobcore/model/amschema/repo.py
#  gobcore/model/events.py
#  gobcore/model/metadata.py
#  gobcore/model/migrations/__init__.py
#  gobcore/model/name_compressor.py
#  gobcore/model/pydantic.py
#  gobcore/model/quality.py
#  gobcore/model/relations.py
#  gobcore/model/sa/__init__.py
#  gobcore/model/sa/gob.py
#  gobcore/model/sa/indexes.py
#  gobcore/model/sa/management.py
#  gobcore/model/schema.py
#  gobcore/parse.py
#  gobcore/quality/__init__.py
#  gobcore/quality/config.py
#  gobcore/quality/issue.py
#  gobcore/quality/quality_update.py
#  gobcore/secure/__init__.py
#  gobcore/secure/config.py
#  gobcore/secure/crypto.py
#  gobcore/secure/cryptos/__init__.py
#  gobcore/secure/cryptos/aes.py
#  gobcore/secure/cryptos/config.py
#  gobcore/secure/cryptos/fernet.py
#  gobcore/secure/request.py
#  gobcore/secure/user.py
#  gobcore/sources/__init__.py
#  gobcore/standalone.py
#  gobcore/status/__init__.py
#  gobcore/status/heartbeat.py
#  gobcore/typesystem/__init__.py
#  gobcore/typesystem/gob_geotypes.py
#  gobcore/typesystem/gob_secure_types.py
#  gobcore/typesystem/gob_types.py
#  gobcore/typesystem/json.py
#  gobcore/utils.py
#  gobcore/views/__init__.py
#  gobcore/workflow/__init__.py
#  gobcore/workflow/start_commands.py
#  gobcore/workflow/start_workflow.py
)

echo "Running mypy"
# temporary disabled, fix mypy for gobcore/__init__.py first
# mypy "${FILES[@]}"

echo "Running unit tests"
coverage run --source=gobcore -m pytest

echo "Reporting coverage"
coverage report --fail-under=100

echo "Check if black finds no potential reformat fixes"
black --check --diff "${FILES[@]}"

echo "Check for potential import sort"
isort --check --diff "${FILES[@]}"

echo "Running flake8"
flake8 "${FILES[@]}"

echo "Checks complete"
