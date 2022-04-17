# COMP3311 22T1 Ass2 ... print info about different releases for Movie

import sys
import psycopg2
from helper_functions import clean

usage = "Usage: q2.py 'PartialMovieTitle'"
db = None
cur = None

argc = len(sys.argv)

try:
    db = psycopg2.connect("dbname=imdb")
    if argc != 2:
        raise ValueError
    cur = db.cursor()
    title = sys.argv[1].replace("'", "\\'")
    qry1 = f"""select id, rating, title, start_year from movies
where title ~* E'{title}'
order by rating desc, start_year, title""" 
    cur.execute(qry1)
    tuples1 = clean(cur.fetchall())
    if len(tuples1) == 0:
        print(f"No movie matching '{sys.argv[1]}'")
    elif len(tuples1) > 1:
        print(f"Movies matching '{sys.argv[1]}'")
        print("===============")
        for tuple in tuples1:
            print(f"{tuple[1]} {tuple[2]} ({tuple[3]})")
    else:
        qry2 = f"""select local_title, region, language, extra_info from aliases
where movie_id = {int(tuples1[0][0])}
order by ordering"""
        cur.execute(qry2)
        tuples2 = clean(cur.fetchall())
        if len(tuples2) > 0:
            print(f"{tuples1[0][2]} ({tuples1[0][3]}) was also released as")
            for tuple in tuples2:
                print(f"'{tuple[0]}'", end="")
                if tuple[1] == None and tuple[2] == None and tuple[3] == None:
                    continue
                elif tuple[1] == None and tuple[2] == None:
                    print(f" ({tuple[3]})")
                else:
                    string = ''
                    if tuple[1] != None:
                        string += f"region: {tuple[1]}"
                    if tuple[1] != None and tuple[2] != None:
                        string += ", "
                    if tuple[2] != None:
                        string += f"language: {tuple[2]}"
                    if len(string) != 0:
                        print(f" ({string})")
        else:
            print(f"{tuples1[0][2]} ({tuples1[0][3]}) has no alternative releases")
except psycopg2.Error as err:
    print("DB error: ", err)
except ValueError: 
    print(usage)
finally:
    if cur:
        cur.close()
    if db:
        db.close()
