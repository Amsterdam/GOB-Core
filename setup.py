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
        'pika',
        'sqlalchemy',
        'geoalchemy2',
        'geomet',
        'shapely',
        'cx-Oracle==7.0.0',
        'datapunt-objectstore',
        'requests==2.20.0',
        'psycopg2-binary==2.7.7',
        'pandas==0.23.3',
    ],
    packages=find_packages(exclude=['tests*']),
    dependency_links=[
        'git+https://github.com/Amsterdam/objectstore.git@v1.0#egg=datapunt-objectstore',
    ],
)
