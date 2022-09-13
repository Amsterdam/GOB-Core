# GOB-Core

GOB shared logic.

Include in a GOB project using:

```bash
pip install -e git+git://github.com/Amsterdam/GOB-Core.git@vX.Y.Z#egg=gobcore
```

If you need to test with a local version of GOB-Core change the include line in `requirements.txt` with:

```bash
-e <<local path to GOB-Core>>
```

# Docker

## Requirements

* docker compose >= 1.25
* Docker CE >= 18.09

## Tests

```bash
docker compose build
docker compose up
```

# Local

## Requirements

* Python >= 3.9
    
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
