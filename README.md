# GOB-Core

GOB shared logic

Include in a GOB project using:

```bash
pip install -e git+git://github.com/Amsterdam/GOB-Core.git@vX.Y.Z#egg=gobcore

```

# Docker

## Requirements

* docker-compose >= 1.17
* docker ce >= 18.03

## Tests

```bash
docker-compose build
docker-compose up
```

# Local

## Requirements

* python >= 3.6
    
## Initialisation

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
    
Or activate the previously created virtual environment

```bash
source venv/bin/activate
```
    
## Tests

Run the tests:

```bash
sh test.sh
```
