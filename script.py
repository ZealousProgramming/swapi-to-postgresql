from dataclasses import asdict
from logging import error
from typing import Final
import os
from venv import create
from xml.dom.pulldom import CHARACTERS
import psycopg2
import asyncio
from aiohttp import ClientSession

# Database info
HOSTNAME: Final[str] = 'localhost'
DATABASE: Final[str] = 'bootcamp'
TABLE_NAME: Final[str] = 'starverse'
USERNAME = os.environ['BOOTCAMP_USER']
PWD = os.environ['BOOTCAMP_CREDS']
PORT_ID: Final[str] = '5433'

COLUMNS = ['name', 'gender', 'height', 'mass', 'birth_year']
COLUMN_TYPES = ['VARCHAR(50)', 'VARCHAR(6)', 'VARCHAR(12)', 'VARCHAR(12)', 'VARCHAR(12)']
COLUMN_HEADER = ''
COLUMN_VALUES = ''
REPLACE_UNKNOWN: bool = True

CHARACTERS = []

async def fetch_people(session, url: str):
	""" Fetches the paginated people collection from the SWAPI API, asynchronously.
		Populates the `CHARACTERS` global array.

			Parameters:
				session (ClientSession): Session interface for making HTTP requests
				url (str): The paginated url to fetch from
	"""
	global CHARACTERS
	async with session.get(url) as response:
		if response.status != 200:
			error("Encountered a problem: %s", response.status)
			return 

		people = await response.json()

		for person in people['results']:
			CHARACTERS.append(person);

		next_page = people['next']
		if next_page != None:
			await fetch_people(session, next_page)

def build_columns():
	""" Builds the table column insert headers, insert value pattern, and CREATE TABLE column declarations.
		Returns the CREATE TABLE column declarations.

			Returns:
				column_decl (str): The column declarations for a CREATE TABLE statement
	"""
	column_decl = 'id SERIAL PRIMARY KEY,'
	global COLUMN_HEADER
	global COLUMN_VALUES
	value = '%s'

	for index, col in enumerate(COLUMNS):
		column_decl += ' ' + col + ' ' + COLUMN_TYPES[index]
		COLUMN_HEADER += col
		COLUMN_VALUES += value

		if index + 1 < len(COLUMN_TYPES):
			column_decl += ','
			COLUMN_HEADER += ', '
			COLUMN_VALUES += ', '

	return column_decl

def create_table(cursor):
	""" Creates the table with the specified `TABLE_NAME` and column declarations from `build_columns`. 
		Returns `True` or `False`, if the table creation was successful or not.

			Parameters:
				cursor (Cursor) : Allows Python to execute PostgreSQL command in a database session

			Returns:
				`True` or `False, if the table creation was successful or not
	"""
	try:
		# Delete a previous iteration of the table, if exists
		drop_table_query = f'''
		DROP TABLE IF EXISTS {TABLE_NAME};
		'''
		cursor.execute(drop_table_query)


		column_decls = build_columns()
		create_table_query = f'''CREATE TABLE {TABLE_NAME} (
			{column_decls}
		);'''

		cursor.execute(create_table_query)

		return True
	except Exception as err:
		print('An exception occurred while creating the table: ', err)
		return False

def populate_table(cursor):
	""" Populates the table with the `CHARACTER` array that was populated by the `fetch_people` function. 
		Returns `True` or `False`, if the table population was successful or not.

			Parameters:
				cursor (Cursor) : Allows Python to execute PostgreSQL command in a database session

			Returns:
				Returns `True` or `False`, if the table population was successful or not.
	"""
	insert_row_query = f'''INSERT INTO {TABLE_NAME} ({COLUMN_HEADER}) VALUES ({COLUMN_VALUES});'''

	try:
		for char in CHARACTERS:
			# Edge case where the API is inconsistent
			name = char['name']
			height = char['height']
			mass = char['mass']
			birth_year = char['birth_year']
			gender = char['gender']

			if char['name'] == 'Jabba Desilijic Tiure':
				gender = 'male'
			else:
				if gender == 'none':
					gender = 'n/a'
			
			if REPLACE_UNKNOWN:
				if height == 'unknown':
					height = "-1"
				if mass == 'unknown':
					mass = "-1"
				if birth_year == 'unknown':
					birth_year = "-1"
			cursor.execute(insert_row_query, (name, gender, height, mass, birth_year))
		return True
	except Exception as err:
		print('An exception occurred while inserting a row into the table: ', err)
		return False

async def main():
	print ('Fetching from SWAPI..')
	async with ClientSession() as session:
		await fetch_people(session, 'https://swapi.dev/api/people')
	
	print ('Fetching complete..')

	try:
		print(f'Connecting to postgresql database at: {HOSTNAME}:{PORT_ID}/{DATABASE}')
		connection = psycopg2.connect(
			host = HOSTNAME,
			dbname = DATABASE,
			user = USERNAME,
			password = PWD,
			port = PORT_ID)

		cursor = connection.cursor()

		print ('Creating table..')
		if not create_table(cursor):
			quit(-1)

		print ('Populating table..')
		if not populate_table(cursor):
			quit(-1)

		print(len(CHARACTERS), ' records inserted')

		connection.commit()
		
	except Exception as err:
		print(err)
	finally:
		if cursor is not None:
			cursor.close()
		if connection is not None:
			connection.close()

if __name__ == '__main__':
	asyncio.run(main())