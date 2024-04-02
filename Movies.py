from cassandra.cluster import Cluster
from datetime import date
from decimal import Decimal
from cassandra.query import SimpleStatement

# Connect to your Cassandra cluster
cluster = Cluster(['localhost'])  # Replace 'localhost' with your Cassandra server's address
session = cluster.connect()

keyspace_name = 'movie_database'
replication_config = {
    'class': 'SimpleStrategy',
    'replication_factor': 5
}

create_keyspace_query = f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {str(replication_config)};"
session.execute(create_keyspace_query)

rows = session.execute(f"SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{keyspace_name}'")

if rows.one():
    print(f"Keyspace '{keyspace_name}' exists.")
else:
    print(f"Keyspace '{keyspace_name}' does not exist.")
    
session.set_keyspace(keyspace_name)    

#------------------Delete tables------------------------------
# tables_to_drop = ['movie', 'actor', 'director', 'studio', 'moviebystudio']
# for table in tables_to_drop:
#     session.execute(f"DROP TABLE IF EXISTS {table}")
#------------------Delete tables------------------------------

def DB_creation_and_insertion ():
#-----CREATE TABLES---------------------------------------------------
    create_movie_table_query_1 = """
        CREATE TABLE IF NOT EXISTS Movie (
            movie_id INT PRIMARY KEY,
            title TEXT,
            year DATE,
            imdb DECIMAL,
            actors SET<INT>,
            director INT,
            studio INT
        )
    """

    session.execute(create_movie_table_query_1)

    #----

    create_actor_table_query_2 = """
        CREATE TABLE IF NOT EXISTS Actor (
            actor_id INT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            birth_year DATE,
        )
    """

    session.execute(create_actor_table_query_2)

    #-----

    create_director_table_query_3 = """
        CREATE TABLE IF NOT EXISTS Director (
            director_id INT PRIMARY KEY,
            dir_first_name TEXT,
            dir_last_name TEXT,
            dir_birth_year DATE,
        )
    """

    session.execute(create_director_table_query_3)

    #-----

    create_studio_table_query_4 = """
        CREATE TABLE IF NOT EXISTS Studio (
            studio_id INT PRIMARY KEY,
            studio_name TEXT,
            established_year DATE
        )
    """

    session.execute(create_studio_table_query_4)
    #-----INSERT DATA---------------------------------------------------
    # Insert data
    insert_data_query_1 = """
        INSERT INTO movie
        (movie_id, title, year, imdb, actors, director, studio)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    # Define the data to insert
    data_1 = (1000, 'Twilight', date.fromisoformat('2008-11-21'), Decimal('5.2'), set(), 0, 0)
    data_2 = (1001, 'Cars', date.fromisoformat('2006-05-26'), Decimal('7.2'), set(), 0, 0)
    data_3 = (1002, 'Pirates of the Caribbean 1', date.fromisoformat('2003-07-09'), Decimal('8.0'), set(), 0, 0)
    session.execute(insert_data_query_1, data_1)
    session.execute(insert_data_query_1, data_2)
    session.execute(insert_data_query_1, data_3)
    #-----

    insert_data_query_2 = """
        INSERT INTO actor
        (actor_id, first_name, last_name, birth_year)
        VALUES (%s, %s, %s, %s)
    """

    data_1 = (2000, 'Johnny', 'Depp', date.fromisoformat('1963-06-09'))
    data_2 = (2001, 'Owen', 'Wilson', date.fromisoformat('1968-11-18'))
    data_3 = (2002, 'Kristen', 'Stewart', date.fromisoformat('1990-04-09'))

    session.execute(insert_data_query_2, data_1)
    session.execute(insert_data_query_2, data_2)
    session.execute(insert_data_query_2, data_3)

    select_query = "SELECT * FROM Actor"

    # Execute the query to retrieve data
    rows = session.execute(select_query)

    # Print the retrieved data
    print('Actor list:')
    for row in rows:
        print(row.actor_id, row.first_name, row.last_name, row.birth_year)
        
    #-----------------Update actors---------------

    if_movie = session.execute("SELECT * FROM movie WHERE movie_id = 1000")

    if if_movie:
            session.execute("UPDATE movie SET actors = actors + {2002} WHERE movie_id = 1000")
            
    if_movie = session.execute("SELECT * FROM movie WHERE movie_id = 1001")
    if if_movie:
            session.execute("UPDATE movie SET actors = actors + {2001} WHERE movie_id = 1001")
            session.execute("UPDATE movie SET actors = actors + {2000} WHERE movie_id = 1001")
            
    if_movie = session.execute("SELECT * FROM movie WHERE movie_id = 1002")
    if if_movie: 
                    session.execute("UPDATE movie SET actors = actors + {2002} WHERE movie_id = 1002")
                    session.execute("UPDATE movie SET actors = actors + {2000} WHERE movie_id = 1002")

    #-----

    insert_data_query_3 = """
        INSERT INTO director
        (director_id, dir_first_name, dir_last_name, dir_birth_year)
        VALUES (%s, %s, %s, %s)
    """

    data_1 = (1, 'John', 'Lasseter', date.fromisoformat('1957-01-12'))
    data_2 = (2, 'Catherine', 'Hardwicke', date.fromisoformat('1955-10-21'))

    session.execute(insert_data_query_3, data_1)
    session.execute(insert_data_query_3, data_2)

    select_query = "SELECT * FROM Director"

    # Execute the query to retrieve data
    rows = session.execute(select_query)

    # Print the retrieved data
    print('Director list:')
    for row in rows:
        print(row.director_id, row.dir_first_name, row.dir_last_name, row.dir_birth_year)

    #---------------UPDATE DIRECTOR-------------------

    # Update movie director list
    if_movie = session.execute("SELECT * FROM movie WHERE movie_id = 1000")

    if if_movie:  # Check if the row exists
            # If the row exists, perform the UPDATE
            session.execute("UPDATE movie SET director = 1 WHERE movie_id = 1000")
            
    if_movie = session.execute("SELECT * FROM movie WHERE movie_id = 1001")

    if if_movie:
            session.execute("UPDATE movie SET director = 1 WHERE movie_id = 1001")

    if_movie = session.execute("SELECT * FROM movie WHERE movie_id = 1002")

    if if_movie:
                session.execute("UPDATE movie SET director = 2 WHERE movie_id = 1002")
    
    #-----
    insert_data_query_4 = """
        INSERT INTO studio
        (studio_id, studio_name, established_year)
        VALUES (%s, %s, %s)
    """

    data_1 = (100, 'Temple Hill Entertainment', date.fromisoformat('2006-01-12'))
    data_2 = (101, 'Walt Disney Pictures', date.fromisoformat('1923-10-21'))

    session.execute(insert_data_query_4, data_1)
    session.execute(insert_data_query_4, data_2)
    select_query = "SELECT * FROM Studio"

    # Execute the query to retrieve data
    rows = session.execute(select_query)

    # Print the retrieved data
    print('Studio list:')
    for row in rows:
        print(row.studio_id, row.studio_name, row.established_year)
        
    #-----------------Update movies studio---------------

    if_movie = session.execute("SELECT * FROM movie WHERE movie_id = 1000")
    if if_movie:  # Check if the row exists
            # If the row exists, perform the UPDATE
            session.execute("UPDATE movie SET studio = 100 WHERE movie_id = 1000")
            
    if_movie = session.execute("SELECT * FROM movie WHERE movie_id = 1001")
    if if_movie:
            session.execute("UPDATE movie SET studio = 100 WHERE movie_id = 1001")
            
    if_movie = session.execute("SELECT * FROM movie WHERE movie_id = 1002")
    if if_movie:
            session.execute("UPDATE movie SET studio = 101 WHERE movie_id = 1002")

    select_query = "SELECT * FROM Movie"

    # Execute the query to retrieve data
    rows = session.execute(select_query)

    # Print the retrieved data
    print('Movie list:')
    for row in rows:
        print(row.movie_id, row.title, row.year, row.imdb, row.director, row.studio)

#--------------------------------------------------------------------------------
#---------------------------QUERIES----------------------------------------------
#--------------------------------------------------------------------------------

def Movie_By_Studio (input_studio):
    # NR 1 # Find movies by studio
    create_MovieByStudio = """
        CREATE TABLE IF NOT EXISTS MovieByStudio (
            studio INT,
            movie_id INT,
            title TEXT,
            year DATE,
            imdb DECIMAL,
            PRIMARY KEY (studio, movie_id)
        )
    """

    session.execute(create_MovieByStudio)

    insert_data_query_1 = """
        INSERT INTO moviebystudio
        (studio, movie_id, title, year, imdb)
        VALUES (%s, %s, %s, %s, %s)
    """
    # Define the data to insert
    data_1 = (100, 1000, 'Twilight', date.fromisoformat('2008-11-21'), Decimal('5.2'))
    data_2 = (101, 1001, 'Cars', date.fromisoformat('2006-05-26'), Decimal('7.2'))
    data_3 = (100, 1002, 'Pirates of the Caribbean 1', date.fromisoformat('2003-07-09'), Decimal('8.0'))
    session.execute(insert_data_query_1, data_1)
    session.execute(insert_data_query_1, data_2)
    session.execute(insert_data_query_1, data_3)

    select_query = f"SELECT * FROM moviebystudio WHERE studio = {input_studio}"

    # Execute the query to retrieve data
    rows = session.execute(select_query)

    # Print the retrieved data
    print('MovieByStudio:')
    for row in rows:
        print(row.studio, row.movie_id, row.title, row.year, row.imdb)

def Movie_By_Title (input_title):
    # NR 1 # Find movies by title
    create_MovieByTitle = """
        CREATE TABLE IF NOT EXISTS MovieByTitle (
            title TEXT,
            movie_id INT,
            year DATE,
            imdb DECIMAL,
            studio INT,
            PRIMARY KEY (title, movie_id)
        )
    """

    session.execute(create_MovieByTitle)

    insert_data_query_1 = """
        INSERT INTO moviebytitle
        (title, movie_id, year, imdb, studio)
        VALUES (%s, %s, %s, %s, %s)
    """
    # Define the data to insert
    data_1 = ('Twilight', 1000, date.fromisoformat('2008-11-21'), Decimal('5.2'), 100)
    data_2 = ('Cars', 1001, date.fromisoformat('2006-05-26'), Decimal('7.2'), 101)
    data_3 = ('Pirates of the Caribbean 1', 1002, date.fromisoformat('2003-07-09'), Decimal('8.0'), 100)
    session.execute(insert_data_query_1, data_1)
    session.execute(insert_data_query_1, data_2)
    session.execute(insert_data_query_1, data_3)

    select_query = f"SELECT * FROM moviebytitle WHERE title = '{input_title}'"

    # Execute the query to retrieve data
    rows = session.execute(select_query)

    # Print the retrieved data
    print('MovieByTitle:')
    for row in rows:
        print(row.title, row.movie_id, row.year, row.imdb, row.studio)


def Director_By_LastName (input_lastname):
    # NR 3 # Find directors by last name
    create_DirectorByLastName = """
        CREATE TABLE IF NOT EXISTS DirectorByLastName (
            director_id INT,
            dir_first_name TEXT,
            dir_last_name TEXT,
            dir_birth_year DATE,
            movies_name SET<TEXT>,
            PRIMARY KEY (dir_last_name, director_id)
        )
    """

    session.execute(create_DirectorByLastName)

    insert_data_query_3 = """
        INSERT INTO directorbylastname
        (director_id, dir_first_name, dir_last_name, dir_birth_year, movies_name)
        VALUES (%s, %s, %s, %s, %s)
    """

    data_1 = (1, 'John', 'Lasseter', date.fromisoformat('1957-01-12'), {'Twilight', 'Cars'} )
    data_2 = (2, 'Catherine', 'Hardwicke', date.fromisoformat('1955-10-21'),  {'Pirates of the Caribbean 1'})

    session.execute(insert_data_query_3, data_1)
    session.execute(insert_data_query_3, data_2)

    select_query = f"SELECT * FROM directorbylastname WHERE dir_last_name = '{input_lastname}'"

    # Execute the query to retrieve data
    rows = session.execute(select_query)

    # Print the retrieved data
    print('DirectorByLastName:')
    for row in rows:
        print(row.dir_last_name, row.director_id, row.dir_first_name, row.dir_birth_year)
        print(f"{row.dir_last_name} Movies: ")
        for movie_name in row.movies_name:
         print(movie_name)


#-----------------------------------------------------------------------------------------
def Actor_By_LastName (input_lastname):
    # NR 4 # Find actors by last name and the movies associated with them
    create_ActorByLastName = """
        CREATE TABLE IF NOT EXISTS ActorByLastName (
            actor_id INT,
            first_name TEXT,
            last_name TEXT,
            birth_year DATE,
            movies_name SET <TEXT>,
            PRIMARY KEY (last_name, actor_id)
        )
    """

    session.execute(create_ActorByLastName)

    insert_data_query_2 = """
        INSERT INTO actorbylastname
        (actor_id, first_name, last_name, birth_year, movies_name)
        VALUES (%s, %s, %s, %s, %s)
    """

    data_1 = (2000, 'Johnny', 'Depp', date.fromisoformat('1963-06-09'), {'Cars', 'Pirates of the Caribbean 1'})
    data_2 = (2001, 'Owen', 'Wilson', date.fromisoformat('1968-11-18'), {'Cars'})
    data_3 = (2002, 'Kristen', 'Stewart', date.fromisoformat('1990-04-09'), {'Twilight', 'Pirates of the Caribbean 1'})

    session.execute(insert_data_query_2, data_1)
    session.execute(insert_data_query_2, data_2)
    session.execute(insert_data_query_2, data_3)

    select_query = f"SELECT * FROM actorbylastname WHERE last_name = '{input_lastname}'"

    # Execute the query to retrieve data
    rows = session.execute(select_query)

    # Print the retrieved data
    print('ActorByLastName:')
    for row in rows:
        print(row.last_name, row.actor_id, row.first_name, row.birth_year)
        print(f"{row.last_name} Movies: ")
        for movie_name in row.movies_name:
         print(movie_name)

#-----------------------------------------------------

#------------------------------------------------------------------------------

if __name__ == "__main__":
    
    DB_creation_and_insertion ()
          
    while True:
        print()
        print("1. Display Movies by Studio [100 | 101]" )
        print("2. Display Actor by Last name [Depp | Wilson | Stewart]")
        print("3. Display Actor by Last name [Lasseter | Hardwicke]")
        print("4. Display Movie by Title [Twilight | Cars | Pirates of the Caribbean 1]")
        print("5. Exit")
        choice = input("Choose an option (1-5): ")

        if choice == "1":
            studio_name = input("Enter studio id: ")
            print()
            Movie_By_Studio (studio_name)
        elif choice == "2":
            input_lastname = input("Enter actor last name: ")
            print()       
            Actor_By_LastName (input_lastname)
        elif choice == "3":
            input_lastname = input("Enter director last name: ")
            print()       
            Director_By_LastName (input_lastname)
        elif choice == "4":
            input_title = input("Enter movie title: ")
            print()       
            Movie_By_Title (input_title)
        elif choice == "5":
            print("Exiting the program.")
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 5.")



# # Close the connection
cluster.shutdown()