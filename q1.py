# COMP3311 22T1 Ass2 ... print num_of_movies, name of top N people with most movie directed

import sys
import psycopg2

usage = "Usage: q1.py [N]"
db = None
N = 10

argc = len(sys.argv)

try:
	db = psycopg2.connect("dbname=imdb")
	if (argc != 3 or type(sys.argv[2]) != int or sys.argv[2] <= 0):
		raise ValueError
	elif (argc == 3):
		N = sys.argv[2]
	cur = db.cursor()
	qry = f"""select count(*), n.name from names as n
			  inner join crew_roles as cr on n.id = cr.name_id
			  where cr.role = 'director'
			  group by n.name
			  order by count(*) desc limit {N};"""
	cur.execute(qry)
	for tuple in cur.fetchall():
		print(f"{tuple[0]} {tuple[1]}")
	cur.close()
except psycopg2.Error as err:
	print("DB error: ", err)
except ValueError:
	print(usage)
finally:
	if db:
		db.close()
