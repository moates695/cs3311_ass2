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
where p.name_id = {tuples1[0][0]};"""
        cur.execute(qry2)
        rating = cur.fetchone()[0]
        if rating == None:
            rating = 0
        else:
            rating = float(rating)
        qry3 = f"""select mg.genre from movie_genres as mg
inner join movies as m on mg.movie_id = m.id
inner join principals as p on m.id = p.movie_id
where p.name_id = {tuples1[0][0]}
group by genre
order by count(*) desc, mg.genre;"""
        cur.execute(qry3)
        genres = clean(cur.fetchmany(3))
        print(f"Filmography for {tuples1[0][1]} {lifespan(tuples1[0])}")    
        print("===============")
        print(f"Personal Rating: {rating}")
        print(f"Top 3 Genres:")
        for genre in genres:
            print(f" {genre[0]}")
        print("===============")
        movies = {}
        qry4 = f"""select m.id, m.title, m.start_year from movies as m
inner join principals as p on m.id = p.movie_id
where p.name_id = {tuples1[0][0]}
order by m.start_year, m.title;"""
        cur.execute(qry4)
        tuples2 = clean(cur.fetchall())
        for movie in tuples2:
            movies[movie[0]] = {}
            movies[movie[0]]['title'] = movie[1]
            movies[movie[0]]['year'] = movie[2]
            movies[movie[0]]['played'] = []
            movies[movie[0]]['crew'] = []
        qry5 = f"""select ar.movie_id, ar.played from acting_roles as ar
where ar.name_id = {tuples1[0][0]}
order by ar.played;"""
        cur.execute(qry5)
        acting_roles = clean(cur.fetchall())
        for played in acting_roles:
            if played[0] in movies.keys():
                movies[played[0]]['played'].append(played[1])
        qry6 = f"""select cr.movie_id, cr.role from crew_roles as cr
where cr.name_id = {tuples1[0][0]}
order by cr.role;"""
        cur.execute(qry6)
        crew_roles = clean(cur.fetchall())
        for crew in crew_roles:
            if crew[0] in movies.keys():
                movies[crew[0]]['crew'].append(crew[1])
        for movie in movies.values():
            print(f"{movie['title']} ({movie['year']})")
            if len(movie['played']) > 0:
                for role in movie['played']:
                    print(f" playing {role}")
            if len(movie['crew']) > 0:
                for role in movie['crew']:
                    print(f" as {role[0].upper() + role[1:].replace('_', ' ')}")
    else:
        if year == None:
            print(f"Names matching '{sys.argv[1]}'")
        else:
            print(f"Names matching '{sys.argv[1]}' {year}")
        print("===============")
        for person in tuples1:
            print(f"{person[1]} {lifespan(person)}")
except psycopg2.Error as err:
    print("DB error: ", err)
except ValueError:
    print(usage)
finally:
    if cur:
        cur.close()
    if db:
        db.close()

