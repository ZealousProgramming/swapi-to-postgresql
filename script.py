from logging import error
import pathlib
from re import sub
from typing import Final
from pathlib import Path
from sys import argv
import os
import json
from unicodedata import category
import psycopg2
import asyncio
from aiohttp import ClientSession

# Database info
HOSTNAME: Final[str] = 'localhost'
DATABASE: Final[str] = 'bootcamp_sw'
TABLE_NAME: Final[str] = 'people'
USERNAME = os.environ['BOOTCAMP_USER']
PWD = os.environ['BOOTCAMP_CREDS']
PORT_ID: Final[str] = '5433'

# Options
FORMAT: bool = True
VERBOSE: bool = False
CACHE: bool = True
FORCE_CACHE_UPDATE: bool = False

SWAPI_API_URL: Final[str] = 'https://swapi.dev/api/'
CATEGORY_NAMES = ['films', 'people', 'planets', 'species', 'starships', 'vehicles']
DATA = {}
SCHEMA = {
	'some_table_no_key': { 
		'header': "",
		'value_format': "",
		'generated_key': False,
		'key': [],
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
			}
		]
	},
	'some_table': { 
		'header': "",
		'value_format': "",
		'generated_key': False,
		'key': [
			'episode_id',
			'title'
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
			}
		]
	},
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
	}
}



async def fetch_category(session, category_name: str):
	catergory_entries = []

	await fetch_page(session, SWAPI_API_URL + category_name + '/', catergory_entries)

	DATA[category_name] = catergory_entries

	if VERBOSE:
		print(DATA[category_name])

async def fetch_page(session: ClientSession, url: str, container):
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
	if not Path(path).exists():
		pathlib.Path(path).mkdir(parents=True, exist_ok=True)

	with open(path + '/' + filename + '.json', 'w') as target_file:
		json.dump(DATA, target_file, indent=4)

def read_from_file(filename: str, path: str):
	global DATA
	with open(path + '/' + filename + '.json') as target_file:
		DATA = json.load(target_file)

def build_columns(category_name):
	""" Builds the table column insert headers, insert value pattern, and CREATE TABLE column declarations.
		Returns the CREATE TABLE column declarations.

			Returns:
				column_decl (str): The column declarations for a CREATE TABLE statement
	"""
	global SCHEMA

	generated_key: bool = SCHEMA[category_name]['generated_key']
	key: list = SCHEMA[category_name]['key']
	has_primary_key: bool = True if len(key) > 0 else False
	column_types = SCHEMA[category_name]['columns']
	column_decl = ''
	column_header = ''
	column_format = ''
	value = '%s'
	type_count = len(column_types)

	for index, col in enumerate(column_types):
		name = col['column_name']
		column_type = col['column_type']
		column_decl += ' ' + name + ' ' + column_type
		
		not_end = index + 1 < type_count

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

def create_table(cursor, table_name):
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
		DROP TABLE IF EXISTS {table_name};
		'''
		cursor.execute(drop_table_query)


		column_decls = build_columns(table_name)
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

		create_table_query = f'''CREATE TABLE {table_name} (
			{column_decls}
			{comp_key}
		);'''

		cursor.execute(create_table_query)

		return True
	except Exception as err:
		print('An exception occurred while creating the table: ', err)
		return False

def populate_table(cursor, category_name):
	""" Populates the table with the `CHARACTER` array that was populated by the `fetch_people` function. 
		Returns `True` or `False`, if the table population was successful or not.

			Parameters:
				cursor (Cursor) : Allows Python to execute PostgreSQL command in a database session

			Returns:
				Returns `True` or `False`, if the table population was successful or not.
	"""
	schema = SCHEMA[category_name]
	collection = DATA[category_name]
	has_generated_key: bool = schema['generated_key']
	key: list = schema['key']
	insert_row_query = f'''INSERT INTO {category_name} ({schema['header']}) VALUES ({schema['value_format']});'''
	unknown = 'unknown'
	none = 'none'
	na = 'n/a'

	try:
		for entry in collection:
			data = ()
			for column in schema['columns']:
				column_name = column['column_name']
				if has_generated_key and column_name in key:
					continue
				entry_data = entry[column_name]

			# Make the columns with 'unknown's consistent
				if FORMAT:
					if type(entry_data).__name__ == "str":
						if entry_data == unknown or entry_data == none:
							entry_data = na
				data += (entry_data,)

			cursor.execute(insert_row_query, data)
		return True
	except Exception as err:
		print('An exception occurred while inserting a row into the table: ', err)
		return False

def print_help():
	""" Prints out the help menu to the console. """

	print('Usage: python ./script.py ([options])')
	print('Example:')
	print('\tpython ./script.py -c')
	print('\tpython ./script.py -f')
	print('\tpython ./script.py --cache')
	print('\tpython ./script.py --force')
	print('\tpython ./script.py -c -v')
	print('\tpython ./script.py -c --verbose')
	print('\tpython ./script.py --cache --verbose')
	print('\n')
	print('Options:')
	print('-c, --cache\t\tWhether or not to cache and use the cached results. Default=true')
	print('\t\t\tDisable caching and use of any existing cache:')
	print('\t\t\t-c=false')
	print('\t\t\t--c=false')
	print("-f, --force\t")
	print('-v, --verbose\t\tPrints out detailed information of the process')
	print('')
	print('General Options: ')
	print('-h, --help\t\tPrint usage information')

async def main():
	global VERBOSE, FORCE_CACHE_UPDATE, CACHE
	filename = 'swapi_data'
	bin_location = './bin'

	# Handle args
	if '-h' in argv or '--help' in argv:
		print_help()
		return

	arg_len: int = len(argv)

	if arg_len > 1:
		for i, arg in enumerate(argv):
			la = arg.lower()
			if '-v' == la or '--verbose' == la:
				VERBOSE = True
			elif la.startswith('-c=') or la.startswith('--cache='):
				count: int = la.count('=');
				
				if count == 1:
					index: int = la.index('=') + 1
					value: str = la[index:].lower()

					if (value == 'false' or value == 'f'):
						CACHE = False
					elif (value == 'true' or value == 't'):
						print('Redundant cache flag as caching is on by default')
				elif count > 1:
					print('Invalid syntax: Too many = in the flag %s', la)
					return
			elif '-f' == la or '--force' == la:
				FORCE_CACHE_UPDATE = True

	if FORCE_CACHE_UPDATE and not CACHE:
		print('Cannot force a cache update and not have caching enabled.')
		print('Please remove the flag to disable caching if you desire to update the existing cache.')
		return

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

		print ('Creating table..')
		if not create_table(cursor, 'people'):
			quit(-1)

		print ('Populating table..')
		if not populate_table(cursor, 'people'):
			quit(-1)

		# print('\n')
		# if not create_table(cursor, 'films'):
		# 	quit(-1)
		# print('\n')
		# if not create_table(cursor, 'some_table'):
		# 	quit(-1)
		# print('\n')
		# if not create_table(cursor, 'some_table_no_key'):
		# 	quit(-1)


		# print ('Populating table..')
		# if not populate_table(cursor, SCHEMA['some_table']):
		# 	quit(-1)

		# print(len(DATA['people']), ' records inserted')

		connection.commit()
		
	except Exception as err:
		print('Encountered an error: ', err.__class__.__name__)
	finally:
		if cursor is not None:
			cursor.close()
		if connection is not None:
			connection.close()

if __name__ == '__main__':
	asyncio.run(main())