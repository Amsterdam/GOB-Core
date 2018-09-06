# GOB-Core
GOB Core components

## Requirements

    * docker-compose >= 1.17
    * docker ce >= 18.03
    * python >= 3.6
    
## Local development

Create a virtual environment:

    python3 -m venv venv
    source venv/bin/activate
    pip install -r src/requirements.txt
    
Or activate the previously created virtual environment

    source venv/bin/activate

Run the tests:

```bash
pytest
pytest --cov=gobcore
pytest --cov=gobcore --cov-report html --cov-fail-under=70
```

The coverage results can be viewed in your browser by opening htmlcov/index.html in yur browser


The tests can also be run by starting the test shell script:

```bash
sh test.sh
```

## Docker

```bash
docker-compose build
docker-compose up
```

## Use GOB-Core in a project

Include in a GOB project using:

```bash
pip install -e git+git://github.com/Amsterdam/GOB-Core.git#egg=gobcore

```

To link to the latest release remove the commit id as follows:

```bash
pip freeze --local > src/requiremenets.txt
```

Example:
In requirements.txt:
```
-e git://github.com/Amsterdam/GOB-Core.git@909301b10ec20f839d1db13fe2bf512ab56c3960#egg=gobcore
```
becomes
```
-e git://github.com/Amsterdam/GOB-Core.git#egg=gobcore
```

