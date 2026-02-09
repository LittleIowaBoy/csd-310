import mysql.connector 
from mysql.connector import errorcode

import dotenv
from dotenv import dotenv_values


secrets = dotenv_values(".env")

config = {
    "user": secrets["USER"],
    "password": secrets["PASSWORD"],
    "host": secrets["HOST"],
    "database": secrets["DATABASE"],
    "raise_on_warnings": True
}




def show_films(cursor, title):
    
    cursor.execute("""select film_name as Name, film_director as Director, genre_name as Genre, studio_name as 'Studio Name'
                    from film INNER JOIN genre ON film.genre_id=genre.genre_id 
                   INNER JOIN studio ON film.studio_id=studio.studio_id""")

    films = cursor.fetchall()
    print("\n--- {} ---".format(title))
    for film in films:
        print("  Film Name: {}\n  Director: {}\n  Genre: {}\n  Studio Name: {}\n".format(film[0], film[1], film[2], film[3]))

def main():
    
    try:
        db = mysql.connector.connect(**config)
        cursor = db.cursor()
        print("\n  Database user {} connected to MySQL on host {} with database {}".format(config["user"], config["host"], config["database"]))
        input("\n\n  Press any key to continue...")
        show_films(cursor, "DISPLAYING FILMS")
        cursor.execute("""INSERT INTO film (film_name, film_releaseDate, film_runtime, film_director, studio_id, genre_id) 
                       VALUES ("Avengers: Infinity War", "2018", 149, "Anthony and Joe Russo", 1, 2);""") 
                       
        db.commit()
        show_films(cursor, "DISPLAYING FILMS AFTER INSERT")
        cursor.execute("UPDATE film SET genre_id = 1 WHERE film_id = 2")
        show_films(cursor, "DISPLAYING FILMS AFTER UPDATE - Changed Alien to Horror")
        cursor.execute("DELETE FROM film WHERE film_name = 'Gladiator'")
        show_films(cursor, "DISPLAYING FILMS AFTER DELETE")

       
    
    except mysql.connector.Error as err:
        print("Error: {}".format(err))
        print("  Unable to connect to the database")
    db.close()



if __name__ == "__main__":
    main()