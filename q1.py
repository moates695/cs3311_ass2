# COMP3311 22T1 Ass2 ... print num_of_movies, name of top N people with most movie directed

import sys
import psycopg2

def clean(old_tuples):
    new_tuples = []
    for tuple in old_tuples:
        new_entry = []
        for entry in tuple:
            if type(entry) == str:
                new_entry.append(entry.strip())
            else:
                new_entry.append(entry)
        new_tuples.append(new_entry)
    return new_tuples

usage = "Usage: q1.py [N]"
db = None
N = 10

argc = len(sys.argv)

try:
    db = psycopg2.connect("dbname=imdb")
    if (argc == 2):
        N = int(sys.argv[1])
        if (N <= 0):
            raise ValueError
    elif (argc > 2):
        raise ValueError
    cur = db.cursor()
    qry = f"""select count(*), n.name from names as n
inner join crew_roles as cr on n.id = cr.name_id
where cr.role = 'director'
group by n.name
order by count(*) desc, n.name limit {N};"""
    cur.execute(qry)
    for tuple in clean(cur.fetchall()):
        print(f"{tuple[0]} {tuple[1]}")
    cur.close()
except psycopg2.Error as err:
    print("DB error: ", err)
except ValueError:
    print(usage)
finally:
    if db:
        db.close()
