import os
from setuptools import setup, find_packages


def replace_env(line: str) -> str:
    if '==' not in line:
        return line

    package, version = line.split('==')
    if version[:2] == '${' and version[-1] == '}':
        version = os.environ[version[2:-1]]

    return f'{package}=={version}'


with open('requirements.txt', mode='r') as reqs:
    install_requires = [replace_env(req.strip()) for req in reqs.readlines()]


setup(
    name='gobcore',
    version='0.1',
    description='GOB Core Components',
    url='https://github.com/Amsterdam/GOB-Core',
    author='Datapunt',
    author_email='',
    license='MPL-2.0',
    install_requires=install_requires,
    packages=find_packages(exclude=['tests*'])
)
