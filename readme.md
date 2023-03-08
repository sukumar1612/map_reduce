# Distributed Map Reduce Framework using Python and Docker

This project is my take on creating a Distributed Map Reduce framework in python

## Setting up the environment

---

1. Using github CLI

```bash
gh repo clone sukumar1612/map_reduce
```

2. Using HTTPS

```bash
git clone https://github.com/sukumar1612/map_reduce.git
```

## Setup project

---

### Virtual environment

* Create a Conda or venv virtual environment with python version `>=3.11`

### Installing Dependencies

* We will be using pip package installer

```bash
pip install -r requirements.txt
``` 
## Docker hub
This repository contains sample containers that could be used for testing purposes
[sg162/map_reduce](https://hub.docker.com/repository/docker/sg162/map_reduce/general)

## Run
To run this project, have docker desktop installed with docker compose

Then run this project using this command
```bash
docker-compose -f docker-compose.yml up -d
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update requirements.txt as appropriate.

## License

[Apache License 2.0](https://choosealicense.com/licenses/apache-2.0/)

## Author

---

* Sukumar G [@Sukumar](https://github.com/sukumar1612)
