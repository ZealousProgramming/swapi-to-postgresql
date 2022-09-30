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
- `DATABASE`: Set the name of the database via the `-db` or `--database` flags
- `PORT_ID`: Set the port to connect to via the `-p` or `--port` flags
- `USERNAME`: An environment variable of the username you use for postgreSQL (DEFAULT: `BOOTCAMP_USER`)
- `PWD`: An environment variable of the password you use for postgreSQL (DEFAULT: `BOOTCAMP_CREDS`)

__Options__: 
- `-db, --database`: Name of the database to connect to
	- DEFAULT: `bootcamp`
	- To change: `-db=some_db` or `--database=some_other_db`
- `-p, --port`: The port to connect to
	- DEFAULT: `5432`
	- To change: `-p=5433` or `--port=5433`
- `-d, --data`: Use a modified version of the dataset
	- To change: `-d="./bin/some_data.json"` or `--data="./bin/some_custom_data.json"`
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
python ./script.py -db=some_database # Connect to a db other than `bootcamp`
python ./script.py -p=5433 # Connect to a db on a port other than `5432`
python ./script.py -c=false # Turn off the use of caching
python ./script.py -d="./bin/custom_data.json" # Use a modified version of the dataset
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
- [x] Import all tables
- [x] Format flag
- [x] Syntax error messages for flag setting
- [x] Update documentation to reflect the refactor
- [x] Flag to use another dbname
- [x] Flag to use another port
- [x] Connection error message
- [x] Allow for a custom data