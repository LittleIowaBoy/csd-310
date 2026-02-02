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

get_studio_table = "SELECT * FROM studio"
get_genre_table = "SELECT * FROM genre"
get_short_movies = "SELECT * FROM film WHERE film_runtime < 120"
get_films_by_director = "SELECT film_name, film_director FROM film ORDER BY film_director"

def main():
    try:
        db = mysql.connector.connect(**config)
        print("\n  Database user {} connected to MySQL on host {} with database {}".format(config["user"], config["host"], config["database"]))
        input("\n\n  Press any key to continue...")
        print("\n --- Displaying Studios ---")
        cursor = db.cursor()
        cursor.execute(get_studio_table)
        for studio in cursor:
            print("  Studio ID: {}\n  Studio Name: {}\n".format(studio[0], studio[1]))
        print("\n --- Displaying Genres ---")
        cursor.execute(get_genre_table)
        for genre in cursor:
            print("  Genre ID: {}\n  Genre Name: {}\n".format(genre[0], genre[1]))
        print("\n --- Displaying Short Movies ---")
        cursor.execute(get_short_movies)
        for movie in cursor:
            print("  Movie Title: {}\n  Runtime: {}\n".format(movie[1], movie[3]))
        print("\n --- Displaying Movies by Director ---")
        cursor.execute(get_films_by_director)
        for movie in cursor:
            print("  Movie Title: {}\n  Director: {}\n".format(movie[0], movie[1]))
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("  The supplied username or password are invalid")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("  The specified database does not exist")
        else:
            print(err)
    db.close()

if __name__ == "__main__":
    main()