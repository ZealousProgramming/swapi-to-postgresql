# swapi-to-postgresql
A utility script to import data from Star Wars API to a local postgreSQL database
![Showcase Image](/screenshots/Capture.PNG)

## Pre-Requisites
- Python 3.8+
- PostgreSQL driver

## Installation
- __SSH__
	`git clone git@github.com:ZealousProgramming/swapi-to-postgresql.git`
- __HTTP__
	`git clone https://github.com/ZealousProgramming/swapi-to-postgresql.git`

## Usage
- `HOSTNAME`: `localhost`
- `DATABASE`: The name of the database (DEFAULT: `bootcamp`)
- `USERNAME`: An environment variable of the username you use for postgreSQL (DEFAULT: `BOOTCAMP_USER`)
- `PWD`: An environment variable of the password you use for postgreSQL (DEFAULT: `BOOTCAMP_CREDS`)
- `PORT_ID` The port postgreSQL is running on

__Options__: 
- `-c, --cache`: Set whether to take advantage of caching
	- DEFAULT: `True`
	- To disable: `-c=false` or `--cache=false` 
- `-f, --force`: Forces a refetch to update the cache
	- DEFAULT: `False`
	- To enable: `-f` or `--force`
- `-fmt, --format`: Set whether it should run the consistency formatter
	- DEFAULT: `True`
	- To disable: `-fmt=false` or `--format=false`
- `-v, --verbose`: Produce and display a detailed output of the process
	- DEFAULT: `False`
	- To enable: `-v` or `--verbose`
``` python

cd swapi-to-postgresql

# In the repo source directory
python ./script.py
python ./script.py -c=false # Turn off the use of caching
python ./script.py -f # Force a cache update
python ./script.py -v # Produce a detailed output
python ./script.py --cache=false
python ./script.py --force
python ./script.py --format=false # Disable consistency formatting
python ./script.py -c=false -v -fmt=false
python ./script.py -c=false --verbose
python ./script.py --cache=false --verbose
```

## Future Features
---
- [x] Import all tables
- [x] Format flag
- [x] Syntax error messages for flag setting
- [x] Update documentation to reflect the refactor
- [ ] Flag to use another dbname
- [ ] Flag to use another port