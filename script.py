from logging import error
import pathlib
from typing import Final
from pathlib import Path
from sys import argv
import sys
import os
import json
import psycopg2
import asyncio
from aiohttp import ClientSession

# Database info
HOSTNAME: Final[str] = 'localhost'
DATABASE: str = 'bootcamp'
USERNAME: str = os.environ['BOOTCAMP_USER']
PWD: str = os.environ['BOOTCAMP_CREDS']
PORT_ID: str = '5432'

# Options
FORMAT: bool = True
VERBOSE: bool = False
CACHE: bool = True
FORCE_CACHE_UPDATE: bool = False

SWAPI_API_URL: Final[str] = 'https://swapi.dev/api/'
CATEGORY_NAMES: list = ['films', 'people', 'planets', 'species', 'starships', 'vehicles']
DATA: dict = {}
SCHEMA: dict = {
	'films': { 
		'header': "",
		'value_format': "",
		'generated_key': False,
		'key': [
			'episode_id'
		],
		'columns': [
			{
				'column_name': 'episode_id',
				'column_type': 'SMALLINT NOT NULL'
			},
			{
				'column_name': 'title',
				'column_type': 'VARCHAR(25) NOT NULL'
			},
			
			{
				'column_name': 'opening_crawl',
				'column_type': 'VARCHAR(575)'
			},
			{
				'column_name': 'director',
				'column_type': 'VARCHAR(25)'
			},
			{
				'column_name': 'producer',
				'column_type': 'VARCHAR(50)'
			},
			{
				'column_name': 'release_date',
				'column_type': 'DATE NOT NULL'
			}
		]
	},
	'people': { 
		'header': '',
		'value_format': '',
		'generated_key': True,
		'key': [
			'id'
		],
		'columns': [
			{
				'column_name': 'id',
				'column_type': 'SERIAL'
			},
			{
				'column_name': 'name',
				'column_type': 'VARCHAR(50)'
			},
			{
				'column_name': 'gender',
				'column_type': 'VARCHAR(18)'
			},
			{
				'column_name': 'height',
				'column_type': 'VARCHAR(12)'
			},
			{
				'column_name': 'mass',
				'column_type': 'VARCHAR(12)'
			},
			{
				'column_name': 'birth_year',
				'column_type': 'VARCHAR(12)'
			},
			{
				'column_name': 'hair_color',
				'column_type': 'VARCHAR(15)'
			},
			{
				'column_name': 'skin_color',
				'column_type': 'VARCHAR(45)'
			},
			{
				'column_name': 'eye_color',
				'column_type': 'VARCHAR(15)'
			},
			{
				'column_name': 'homeworld',
				'column_type': 'VARCHAR(50)'
			}
		]
	},
	'planets': { 
		'header': '',
		'value_format': '',
		'generated_key': True,
		'key': [
			'id'
		],
		'columns': [
			{
				'column_name': 'id',
				'column_type': 'SERIAL'
			},
			{
				'column_name': 'name',
				'column_type': 'VARCHAR(50)'
			},
			{
				'column_name': 'rotation_period',
				'column_type': 'VARCHAR(18)'
			},
			{
				'column_name': 'orbital_period',
				'column_type': 'VARCHAR(18)'
			},
			{
				'column_name': 'diameter',
				'column_type': 'VARCHAR(12)'
			},
			{
				'column_name': 'climate',
				'column_type': 'VARCHAR(30)'
			},
			{
				'column_name': 'gravity',
				'column_type': 'VARCHAR(45)'
			},
			{
				'column_name': 'terrain',
				'column_type': 'VARCHAR(45)'
			},
			{
				'column_name': 'surface_water',
				'column_type': 'VARCHAR(15)'
			},
			{
				'column_name': 'population',
				'column_type': 'VARCHAR(15)'
			}
		]
	},
	'species': { 
		'header': '',
		'value_format': '',
		'generated_key': True,
		'key': [
			'id'
		],
		'columns': [
			{
				'column_name': 'id',
				'column_type': 'SERIAL'
			},
			{
				'column_name': 'name',
				'column_type': 'VARCHAR(25)'
			},
			{
				'column_name': 'classification',
				'column_type': 'VARCHAR(15)'
			},
			{
				'column_name': 'average_height',
				'column_type': 'VARCHAR(10)'
			},
			{
				'column_name': 'skin_colors',
				'column_type': 'VARCHAR(50)'
			},
			{
				'column_name': 'hair_colors',
				'column_type': 'VARCHAR(50)'
			},
			{
				'column_name': 'eye_colors',
				'column_type': 'VARCHAR(50)'
			},
			{
				'column_name': 'average_lifespan',
				'column_type': 'VARCHAR(12)'
			},
			{
				'column_name': 'homeworld',
				'column_type': 'VARCHAR(50)'
			},
			{
				'column_name': 'language',
				'column_type': 'VARCHAR(15)'
			}
		]
	},
	'starships': { 
		'header': '',
		'value_format': '',
		'generated_key': True,
		'key': [
			'id'
		],
		'columns': [
			{
				'column_name': 'id',
				'column_type': 'SERIAL'
			},
			{
				'column_name': 'name',
				'column_type': 'VARCHAR(32)'
			},
			{
				'column_name': 'model',
				'column_type': 'VARCHAR(50)'
			},
			{
				'column_name': 'manufacturer',
				'column_type': 'VARCHAR(80)'
			},
			{
				'column_name': 'cost_in_credits',
				'column_type': 'VARCHAR(15)'
			},
			{
				'column_name': 'length',
				'column_type': 'VARCHAR(12)'
			},
			{
				'column_name': 'max_atmosphering_speed',
				'column_type': 'VARCHAR(12)'
			},
			{
				'column_name': 'crew',
				'column_type': 'VARCHAR(12)'
			},
			{
				'column_name': 'passengers',
				'column_type': 'VARCHAR(12)'
			},
			{
				'column_name': 'cargo_capacity',
				'column_type': 'VARCHAR(15)'
			},
			{
				'column_name': 'consumables',
				'column_type': 'VARCHAR(12)'
			},
			{
				'column_name': 'hyperdrive_rating',
				'column_type': 'VARCHAR(10)'
			},
			{
				'column_name': 'MGLT',
				'column_type': 'VARCHAR(8)'
			},
			{
				'column_name': 'starship_class',
				'column_type': 'VARCHAR(35)'
			}
		]
	},
	'vehicles': { 
		'header': '',
		'value_format': '',
		'generated_key': True,
		'key': [
			'id'
		],
		'columns': [
			{
				'column_name': 'id',
				'column_type': 'SERIAL'
			},
			{
				'column_name': 'name',
				'column_type': 'VARCHAR(32)'
			},
			{
				'column_name': 'model',
				'column_type': 'VARCHAR(50)'
			},
			{
				'column_name': 'manufacturer',
				'column_type': 'VARCHAR(80)'
			},
			{
				'column_name': 'cost_in_credits',
				'column_type': 'VARCHAR(15)'
			},
			{
				'column_name': 'length',
				'column_type': 'VARCHAR(12)'
			},
			{
				'column_name': 'max_atmosphering_speed',
				'column_type': 'VARCHAR(12)'
			},
			{
				'column_name': 'crew',
				'column_type': 'VARCHAR(12)'
			},
			{
				'column_name': 'passengers',
				'column_type': 'VARCHAR(12)'
			},
			{
				'column_name': 'cargo_capacity',
				'column_type': 'VARCHAR(15)'
			},
			{
				'column_name': 'consumables',
				'column_type': 'VARCHAR(15)'
			},
			{
				'column_name': 'vehicle_class',
				'column_type': 'VARCHAR(35)'
			}
		]
	},
}

async def fetch_category(session: ClientSession, category_name: str):
	""" Fetches the category data, from the SWAPI endpoint of name `category_name`, and 
		appends it to the DATA dictionary.

		Parameters:
			session (ClientSession): The app's Psycopg2 session
			category_name (str): The name of the category you wish to fetch
	"""
	catergory_entries: list = []

	await fetch_page(session, SWAPI_API_URL + category_name + '/', catergory_entries)

	DATA[category_name] = catergory_entries

	if VERBOSE:
		print(DATA[category_name])

async def fetch_page(session: ClientSession, url: str, container: list):
	""" Fetches a page of data for a particular category. If there is another page, then 
		it'll automatically recursively call `fetch_page()` until all the pages are collected
		and appended to the passed container.
		

		Parameters:
			session (ClientSession): The app's Psycopg2 session
			container (str): The container to append the new page to
	"""
	async with session.get(url) as response:
		if response.status != 200:
			error("Encountered a problem: %s", response.status)
			return 

		results = await response.json()

		for entry in results['results']:
			container.append(entry)

		next_page = results['next']
		if next_page != None:
			await fetch_page(session, next_page, container)

def write_to_file(filename: str, path: str):
	""" Caches the fetched data at path/filename.json. If the path does not exist, it will be created.

			Parameters:
				filename (str): The root name of the exported file (DEFAULT: `swapi_data`)
				path (str): The path to the directory of the exported file (DEFAULT: `./bin/`)
	"""

	if not Path(path).exists():
		pathlib.Path(path).mkdir(parents=True, exist_ok=True)

	with open(path + '/' + filename + '.json', 'w') as target_file:
		json.dump(DATA, target_file, indent=4)

def read_from_file(filename: str, path: str):
	""" Reads the cached data at path/filename.json. 

			Parameters:
				filename (str): The root name of the exported file (DEFAULT: `swapi_data`)
				path (str): The path to the directory of the exported file (DEFAULT: `./bin/`)
	"""
	global DATA
	with open(path + '/' + filename + '.json') as target_file:
		DATA = json.load(target_file)

def build_columns(category_name) -> str:
	""" Builds the table column insert headers, insert value pattern, and CREATE TABLE column declarations
		for the table for `category_name`.
		Returns the CREATE TABLE column declarations.

			Parameters:
				category_name (str): The name of the category to build to columns for

			Returns:
				column_decl (str): The column declarations for a CREATE TABLE statement
	"""
	global SCHEMA

	generated_key: bool = SCHEMA[category_name]['generated_key']
	key: list = SCHEMA[category_name]['key']
	has_primary_key: bool = True if len(key) > 0 else False
	column_types: list = SCHEMA[category_name]['columns']
	column_decl: str = ''
	column_header: str = ''
	column_format: str = ''
	value: str = '%s'
	type_count: int = len(column_types)

	for index, col in enumerate(column_types):
		name: str = col['column_name']
		column_type: str = col['column_type']
		column_decl += ' ' + name + ' ' + column_type
		
		not_end: bool = index + 1 < type_count

		if not_end or has_primary_key:
			column_decl += ','


		if not generated_key or (generated_key and not name in key):
			column_header += name

			if column_type.startswith('DATE'):
				column_format += f'CAST({value} AS DATE)'
			elif column_type.startswith('SMALLINT'):
				column_format += f'CAST({value} AS SMALLINT)'
			else:
				column_format += value

			if not_end:
				column_header += ', '
				column_format += ', '

	SCHEMA[category_name]['header'] = column_header
	SCHEMA[category_name]['value_format'] = column_format

	return column_decl

def create_table(cursor, table_name) -> bool:
	""" Creates the table with the specified `table_name` and column declarations from `build_columns`. 
		Returns `True` or `False`, if the table creation was successful or not.

			Parameters:
				cursor (Cursor): Allows Python to execute PostgreSQL command in a database session
				table_name (str): The name of the table, typically the same as the category name

			Returns:
				`True` or `False, if the table creation was successful or not
	"""
	try:
		# Delete a previous iteration of the table, if exists
		drop_table_query: str = f'''
		DROP TABLE IF EXISTS {table_name};
		'''
		cursor.execute(drop_table_query)


		column_decls: str = build_columns(table_name)
		comp_key: str = 'PRIMARY KEY ('

		key: list = SCHEMA[table_name]['key']
		sub_key_count: int = len(key)

		if sub_key_count > 1:
			for index, key_part in enumerate(key):
				comp_key += key_part

				if index + 1 < sub_key_count:
					comp_key += ', '
			comp_key += ')'
		elif sub_key_count == 1:
			comp_key += key[0]
			comp_key += ')'
		elif sub_key_count == 0:
			comp_key = ''

		create_table_query: str = f'''CREATE TABLE {table_name} (
			{column_decls}
			{comp_key}
		);'''

		cursor.execute(create_table_query)

		return True

	except Exception as err:
		print('An exception occurred while creating the table: ', err)
		return False

def populate_table(cursor, category_name) -> bool:
	""" Populates the table with the name that matches `category_name` with the fetched or cached data.
		Returns `True` or `False`, if the table population was successful or not.

			Parameters:
				cursor (Cursor): Allows Python to execute PostgreSQL command in a database session
				category_name (str): The name of the table to populate

			Returns:
				Returns `True` or `False`, if the table population was successful or not.
	"""
	schema:dict = SCHEMA[category_name]
	collection: list = DATA[category_name]
	has_generated_key: bool = schema['generated_key']
	key: list = schema['key']
	insert_row_query: str = f'''INSERT INTO {category_name} ({schema['header']}) VALUES ({schema['value_format']});'''

	# Formatting
	unknown: str = 'unknown'
	none: str = 'none'
	na: str = 'n/a'
	upper_na: str = 'N/A'

	try:
		for entry in collection:
			data: tuple = ()

			for column in schema['columns']:
				column_name: str = column['column_name']

				if has_generated_key and column_name in key:
					continue

				entry_data = entry[column_name]

			# Make the columns with 'unknown's consistent
				if FORMAT:
					if type(entry_data).__name__ == "str":
						if entry_data == unknown or entry_data == none or entry_data == upper_na:
							entry_data = na
				data += (entry_data,)

			cursor.execute(insert_row_query, data)
		return True

	except Exception as err:
		print('An exception occurred while inserting a row into the table: ', err)
		return False

def print_help():
	""" Prints out the help menu to the terminal. """

	print('Usage: python ./script.py ([options])')
	print('Example:')
	print('\tpython ./script.py -db=some_database')
	print('\tpython ./script.py -p=5433')
	print('\tpython ./script.py -c=false')
	print('\tpython ./script.py -f')
	print('\tpython ./script.py --cache=false')
	print('\tpython ./script.py --force')
	print('\tpython ./script.py --format=false')
	print('\tpython ./script.py -c=false -v -fmt=false')
	print('\tpython ./script.py -c=false --verbose')
	print('\tpython ./script.py --cache=false --verbose')
	print('\n')
	print('Options:')
	print('-db, --database\t\tName of the database to connect to (DEFAULT="bootcamp")')
	print('\t\t\t\t-db=some_db')
	print('\t\t\t\t-database=some_other_db')
	print('-p, --port\t\tThe port to connect to (DEFAULT=5432)')
	print('\t\t\t\t-p=5433')
	print('\t\t\t\t--port=5434')
	print('-c, --cache\t\tWhether or not to cache and use the cached results (Default=true)')
	print('\t\t\tDisable caching and the use of any existing cache:')
	print('\t\t\t\t-c=false')
	print('\t\t\t\t--cache=false')
	print('-fmt, --format\t\tFormat the fields to be consistent (DEFAULT=true)')
	print('\t\t\t\t-fmt=false')
	print('\t\t\t\t--format=false')
	print('\t\t\tDisable consistency formatting:')
	print("-f, --force\t\tForce a cache update")
	print('-v, --verbose\t\tPrints out detailed information of the process')
	print('')
	print('General Options: ')
	print('-h, --help\t\tPrint usage information')

async def main():
	global VERBOSE, FORCE_CACHE_UPDATE, CACHE, FORMAT, DATABASE, PORT_ID
	filename: str = 'swapi_data'
	bin_location: str = './bin'

	# Handle args
	if '-h' in argv or '--help' in argv:
		print_help()

		return

	arg_len: int = len(argv)

	if arg_len > 1:
		for i, arg in enumerate(argv):
			la: str = arg.lower()

			if '-v' == la or '--verbose' == la:
				VERBOSE = True

			elif la.startswith('-c=') or la.startswith('--cache='):
				count: int = la.count('=')
				
				if count == 1:
					index: int = la.index('=') + 1
					value: str = la[index:].lower()

					if (value == 'false' or value == 'f'):
						CACHE = False

					elif (value == 'true' or value == 't'):
						print('Redundant cache flag as caching is on by default')

					else:
						print('Invalid syntax: %s is not a valid option. Options: true/TRUE/t, false/FALSE/f')
						return

				elif count > 1:
					print('Invalid syntax: Too many = in the flag %s', la)
					return

			elif '-f' == la or '--force' == la:
				FORCE_CACHE_UPDATE = True

			elif la.startswith('-fmt=') or la.startswith('--format='):
				count: int = la.count('=')

				if count == 1:
					index: int = la.index('=') + 1
					value: str = la[index:].lower()

					if value == 'false' or value == 'f':
						FORMAT = False

					elif (value == 'true' or value == 't'):
						print('Redundant format flag as caching is on by default')

					else:
						print('Invalid syntax: %s is not a valid option. Options: true/TRUE/t, false/FALSE/f')
						return


				elif count > 1:
					print('Invalid syntax: Too many = in the flag %s', la)
					return
			elif arg.startswith('-db=') or arg.startswith('--database='):
				count: int = arg.count('=')

				if count == 1:
					index: int = la.index('=') + 1
					value: str = la[index:]

					DATABASE = value
				
				elif count > 1:	
					print('Invalid syntax: Too many = in the flag %s', la)
					return
			elif arg.startswith('-p=') or arg.startswith('--port='):
				count: int = arg.count('=')

				if count == 1:
					index: int = la.index('=') + 1
					value: str = la[index:]

					PORT_ID = value
				
				elif count > 1:	
					print('Invalid syntax: Too many = in the flag %s', la)
					return

	if FORCE_CACHE_UPDATE and not CACHE:
		print('Cannot force a cache update and not have caching enabled.')
		print('Please remove the flag to disable caching if you desire to update the existing cache.')

		return

	if VERBOSE:
		print('Running with Options:')
		print('\t Database:', DATABASE)
		print('\t Database:', PORT_ID)
		print('\t Verbose Output:', VERBOSE)
		print('\t Formatting:', FORMAT)
		print('\t Cache:', CACHE)
		print('\t Force Cache Update:', FORCE_CACHE_UPDATE)

	cache_exists: bool = Path(bin_location + '/' + filename + '.json').exists()

	if not FORCE_CACHE_UPDATE and CACHE and cache_exists:
		print ('Reading from cache..')

		read_from_file(filename, bin_location)

		print ('Reading from cache complete..')

	else:
		if CACHE and not cache_exists and not FORCE_CACHE_UPDATE:
			print("No cache to read from..")

		elif FORCE_CACHE_UPDATE:
			print("Forcing cache update..")

		print ('Fetching from SWAPI..')

		async with ClientSession() as session:
			for category_name in CATEGORY_NAMES:
				await fetch_category(session, category_name)
		
		print ('Fetching complete..')

		if CACHE:
			write_to_file(filename, bin_location)

			print('Exported to: ' + bin_location + "/" + filename + '.json')

	try:
		print(f'Connecting to postgresql database at: {HOSTNAME}:{PORT_ID}/{DATABASE}')

		connection = psycopg2.connect(
			host = HOSTNAME,
			dbname = DATABASE,
			user = USERNAME,
			password = PWD,
			port = PORT_ID)
		cursor = connection.cursor()

		print ('Constructing tables..')

		for category in SCHEMA:
			if VERBOSE:
				print ('Constructing %s table..', category)
			
			if not create_table(cursor, category):
				sys.exit(-1)

		print ('Populating tables..')

		for category in SCHEMA:
			if VERBOSE:
				print ('Populating %s table..', category)

			if not populate_table(cursor, category):
				sys.exit(-1)

			if VERBOSE:	
				print(len(DATA[category]), ' records inserted')

		connection.commit()

	except psycopg2.errors.OperationalError as e:
		print("Failed to connect.. \n{0}".format(e))
		sys.exit(-1)

	except Exception as err:
		# print('Encountered an error: ', err.__class__.__name__)
		print('Encountered an error: ', err)
		sys.exit(-1)

	finally:
		if cursor is not None:
			cursor.close()

		if connection is not None:
			connection.close()

if __name__ == '__main__':
	asyncio.run(main())