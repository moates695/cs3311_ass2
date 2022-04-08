# COMP3311 22T1 Ass2 ... print info about cast and crew for Movie

import sys
import psycopg2
from helper_functions import clean

usage = "Usage: q3.py 'MovieTitlePattern' [Year]"
db = None
cur = None
year = None

argc = len(sys.argv)

try:
    db = psycopg2.connect("dbname=imdb")
    if argc == 1 or argc > 3:
        raise ValueError
    elif argc == 3:
        year = int(sys.argv[2])
    cur = db.cursor()
    title = sys.argv[1].replace("'", "\\'")
    if year == None:
        qry1 = f"""select id, rating, title, start_year from movies
where title ~* E'{title}'
order by rating desc, start_year, title;"""
    else:
        qry1 = f"""select id, rating, title, start_year from movies
where title ~* E'{title}'
and start_year = {year}
order by rating desc, start_year, title;"""
    cur.execute(qry1)
    tuples1 = clean(cur.fetchall())
    if len(tuples1) == 0:
        if year == None:
            print(f"No movie matching '{sys.argv[1]}'")
        else:
            print(f"No movie matching '{sys.argv[1]}' {year}")
    elif len(tuples1) == 1:
        qry2 = f"""select n.name, ar.played from acting_roles as ar 
inner join names as n on ar.name_id = n.id
inner join principals as p on n.id = p.name_id
where ar.movie_id = {tuples1[0][0]}
and p.movie_id = {tuples1[0][0]}
order by p.ordering, ar.played;"""
        cur.execute(qry2)
        actors = clean(cur.fetchall())
        qry3 = f"""select n.name, cr.role from crew_roles as cr
inner join names as n on cr.name_id = n.id
inner join principals as p on n.id = p.name_id
where cr.movie_id = {tuples1[0][0]}
and p.movie_id = {tuples1[0][0]}
order by p.ordering, cr.role;"""
        cur.execute(qry3)
        crew = clean(cur.fetchall())
        print(f"{tuples1[0][2]} ({tuples1[0][3]})")
        print("===============")
        print("Starring")
        for actor in actors:
            print(f" {actor[0]} as {actor[1]}")
        print("and with")
        for member in crew:
            print(f" {member[0]}: {member[1][0].upper() + member[1][1:].replace('_', ' ')}")
    else:
        if year == None:
            print(f"Movies matching '{sys.argv[1]}'")
        else:
            print(f"Movies matching '{sys.argv[1]}' {year}")
        print("===============")
        for tuple in tuples1:
            print(f"{tuple[1]} {tuple[2]} ({tuple[3]})")
except psycopg2.Error as err:
    print("DB error: ", err)
except ValueError:
    print(f'{usage}')
finally:
    if cur:
        cur.close()
    if db:
        db.close()
