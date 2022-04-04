# COMP3311 22T1 Ass2 ... get Name's biography/filmography

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

def lifespan(person):
    if person[2] == None:
        lifespan = "(???)"
    elif person[3] == None:
        lifespan = f"({person[2]}-)"
    else:
        lifespan = f"({person[2]}-{person[3]})"
    return lifespan

usage = "Usage: q4.py 'NamePattern' [Year]"
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
    name = sys.argv[1].replace("'", "\\'")
    if year == None:
        qry1 = f"""select id, name, birth_year, death_year from names
where name ~* E'{name}'
order by name, birth_year, id;"""
    else:
        qry1 = f"""select id, name, birth_year, death_year from names
where name ~* E'{name}'
and birth_year = {year}
order by name, birth_year, id;"""
    cur.execute(qry1)
    tuples1 = clean(cur.fetchall())
    #print(len(tuples1))
    #print(qry1)
    if len(tuples1) == 0:
        if year == None:
            print(f"No name matching '{sys.argv[1]}'")
        else:
            print(f"No name matching '{sys.argv[1]}' {year}")
    elif len(tuples1) == 1:
        qry2 = f"""select round(avg(m.rating)::decimal, 1) from movies as m
inner join principals as p on m.id = p.movie_id
inner join names as n on p.name_id = n.id
where n.id = {tuples1[0][0]};"""
        cur.execute(qry2)
        rating = cur.fetchone()[0]
        if rating == None:
            rating = 0
        else:
            rating = float(rating)
        qry3 = f"""select mg.genre from movie_genres as mg
inner join movies as m on mg.movie_id = m.id
inner join principals as p on m.id = p.movie_id
inner join names as n on p.name_id = n.id
where n.id = {tuples1[0][0]}
group by genre
order by count(*) desc, mg.genre;"""
        cur.execute(qry3)
        genres = clean(cur.fetchmany(3))
        print(f"Filmography for {tuples1[0][1]} ({lifespan(tuples1[0])})")    
        print("===============")
        print(f"Personal rating: {rating}")
        print(f"Top 3 Genres:")
        for genre in genres:
            print(f" {genre[0]}")
    else:
        if year == None:
            print(f"Names matching '{sys.argv[1]}'")
        else:
            print(f"Names matching '{sys.argv[1]}' {year}")
        print("===============")
        for person in tuples1:
            print(f"{person[0]} {lifespan(person)}")
except psycopg2.Error as err:
    print("DB error: ", err)
except ValueError:
    print(usage)
finally:
    if cur:
        cur.close()
    if db:
        db.close()

