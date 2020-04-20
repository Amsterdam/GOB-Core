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
        'shapely',
        'cx-Oracle==7.1.2',
        'xlrd==1.2.0',
        'datapunt-objectstore',
        'requests==2.20.0',
        'psycopg2-binary==2.7.7',
        'pandas==0.23.4',
        'ijson==2.3',
        'cryptography==2.8',
        'pycryptodome==3.9.4'

    ],
    packages=find_packages(exclude=['tests*']),
    dependency_links=[
        'git+https://github.com/Amsterdam/objectstore.git@v1.0#egg=datapunt-objectstore',
    ],
)
