# COMP3311 22T1 Ass2 ... print num_of_movies, name of top N people with most movie directed

import sys
import psycopg2

# define any local helper functions here

# set up some globals

usage = "Usage: q1.py [N]"
db = None

# process command-line args

argc = len(sys.argv)

# manipulate database

try:
	db = psycopg2.connect("dbname=imdb")
	# ... add your code here ...
except psycopg2.Error as err:
	print("DB error: ", err)
finally:
	if db:
		db.close()
