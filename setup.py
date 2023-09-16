"""Setup script for building the GOB-Core distribution."""

import os
from setuptools import setup


def replace_env(line: str) -> str:
    """Honor environment variables (${LIBGDAL_VERSION}) in package versions."""
    if "==" not in line:
        return line

    package, version = line.split("==")
    if version[:2] == "${" and version[-1] == "}":
        version = os.environ[version[2:-1]]

    return f"{package}=={version}"


with open("requirements.txt", mode="r", encoding="utf-8") as reqs:
    install_requires = [replace_env(req.strip()) for req in reqs.readlines()]


setup(install_requires=install_requires)
