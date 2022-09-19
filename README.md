# swapi-to-postgresql
A utility script to import `people` from Star Wars API to a local postgreSQL database
![Showcase Image](/screenshots/Capture.PNG)

## Pre-Requisites
- Python 3.8+
- PostgreSQL driver

## Installation
- __SSH__
	`git clone git@github.com:<username>/swapi-to-postgresql.git`
- __HTTP__
	`git clone https://github.com/<username>/swapi-to-postgresql.git`

## Usage
- `HOSTNAME`: `localhost`
- `DATABASE`: The name of the database (DEFAULT: `bootcamp`)
- `TABLE_NAME`: The name of the table(DEFAULT: `starverse`)
- `USERNAME`: An environment variable of the username you use for postgreSQL (DEFAULT: `BOOTCAMP_USER`)
- `PWD`: An environment variable of the password you use for postgreSQL (DEFAULT: `BOOTCAMP_CREDS`)
- `PORT_ID` The port postgreSQL is running on
``` python
cd swapi-to-postgresql

# In the repo source directory
python ./script.py
```