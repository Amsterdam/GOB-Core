import os

ENV = {
    'TENANT_NAME': 'ten_name',
    'TENANT_ID': 'ten_id',
    'OBJECTSTORE_USER': 'user',
    'OBJECTSTORE_PASSWORD': 'pw',
    'OBJECTSTORE_LOCAL': 'local'
}
os.environ.update(ENV)
