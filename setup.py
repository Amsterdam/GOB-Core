from setuptools import setup, find_packages

setup(
    name='gobcore',
    version='0.1',
    description='GOB Core Components',
    url='https://github.com/Amsterdam/GOB-Core',
    author='Datapunt',
    author_email='',
    license='MPL-2.0',
    install_requires=[
        'pika==0.12.0',
        'sqlalchemy==1.3.3',
        'geoalchemy2',
        'geomet',
        'shapely==1.7.1',
        'cx-Oracle==7.3.0',
        'xlrd==1.2.0',
        'datapunt-objectstore==2020.9.7',
        'requests==2.20.0',
        'psycopg2-binary==2.7.7',
        'pandas==0.23.4',
        'paramiko==2.7.1',
        'ijson==2.3',
        'cryptography==3.2.1',
        'pycryptodome==3.9.4',
        'pyodbc==4.0.30',
        'GDAL==2.4.4',
    ],
    packages=find_packages(exclude=['tests*']),
    dependency_links=[
        'git+https://github.com/Amsterdam/objectstore.git@v1.0#egg=datapunt-objectstore',
    ],
)
