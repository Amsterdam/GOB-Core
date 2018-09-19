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
        'geomet'
    ],
    packages=find_packages(exclude=['tests*'])
)
