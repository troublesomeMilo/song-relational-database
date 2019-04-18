##Data Modeling with Postgres - Song Play Database
By Harley Hutchins

###Objective
The objective of this repository is to implement an effective data model for a Postgres database. The input data comes from 1) a subet of a database of songs and 2) a simulated song play log. The scripts in this repository will perform the following actions:

1. Create database and tables
2. Extract data from song and log dataset files
3. Transform data when applicable
4. Load data into proper tables of the schema

###Inputs
Raw data for this project comes from two sources, a database of song information and a simulated log of song plays.

####Songs Dataset
The songs dataset consists of a small subset of the Million Songs Dataset, a database of song information including the following metadata of note:

- Song_id: A unique song identifier from the Echo Nest 
- Artist_id: A unique artist identifier from the Echo Nest
- title: Song title
- duration: Song duration in minutes
- year: Year the song was released
- artist_name: Name of the artist
- artist_location: Location of the artist
- artist_latitude: Geographic latitude of artist's location
- artist_longitude: Geographic longitude of artist's location

The song data comes in the form of a JSON file unique to each song. Thus a tool for extractig data from JSON files will be needed. The songs used for this project are stored in a tree of nested folders. Thus a tool for searching through a number of folders to find the JSON files (and their filepaths) will be needed. 

####Log Dataset
An event simulator was used (in conjunction with the subset of songs from the Song Dataset) to generate a series of event logs of song plays by random users.  Each event log includes the following metadata of note:

- artist: Name of the artist
- firstName: First name of the user
- gender: Gender of the user
- lastName: Last name of the user
- level: Subscription level of user
- location: Location of user
- page: Webpage originating the song play
- sessionid: Session identifier
- ts: UTC timestamp
- userAgent: System/browser information of user
- userid: A unique user identifier

The dataset is partitioned by year/month/day, with a unique JSON event log file for each day in the month of November, 2018. Like the songs dataset, the event log files are stored in a tree of nested folders. Like the songs dataset, a method of searching for and reading from JSON files will be needed.

###Methodology

####Schema
A simple star schema is used to model the data. The fact table will be constructed from information pulled from the log dataset. The surrounding dimension tables are built from both the log dataset and songs dataset. The schema is as follows:

***Fact Table***

**songplays**
- start_time: UTC time of song play
- user_id: Unique user identifier
- level: Subscription level of user
- song_id: Unque song identifier
- artist_id: Unique artist identifier
- session_id: Unique session identifier
- location: User location
- user_agent: User system/browser information

***Dimension Tables***

**users**
- user_id: Unique user identifier
- first_name: First name of user
- last_name: Last name of user
- gender: Gender of user
- level: Subscription level of user

**songs**
- song_id: Unique song identifier
- title: Title of song
- artist_id: Unique artist identifier
- year: Year of song release
- duration: Song duration in minutes

**artist**
- artist_id: Unique artist identifier
- name: Name of artist
- location: Location of artist
- latitude: Geographic latitude of artist
- longitude: Geographic longitude of artist

**time**
- start_time: UTC start time of song play
- hour: Hour of song play
- day: Day of song play
- week: Week of song play
- month: Month of song play
- year: year of song play
- weekday: Weekday of song play

####Creation of Database and Tables
Interaction with Postgres is achieved via the Psycopg2 python wrapper. A python script called `create_tables.py` is used to create the database and tables. It imports `psycopg2` for Postgres execution and imports a variety of variables from a script called `sql_queries.py`, which is discussed later. The `create_tables.py` script defines the function `create_database()` to connect to Postgres, generate a cursor, drop the database if it exists, create a new instance of the database, drop connection, reestablish connection and cursor.

    def create_database():
        # connect to default database
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    
        # create sparkify database with UTF8 encoding
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    
        # close connection to default database
        conn.close()    
    
        # connect to sparkify database
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    
        return cur, conn
        
he `create_tables.py` script then defines the function `drop-tables(cur, conn)` to drop any existing tables to ensure a clean slate. 

    def drop_tables(cur, conn):
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()

Note that the function references a variable `drop_table_queries`, which is imported from the `sql_queries.py` module that is discussed below. The `create_tables.py` script then defines the function `create_tables(cur, conn)` to generate the tables of the schema discussed above. 

    def create_tables(cur, conn):
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()

Note that the function references a variable `create_table_queries`, which is imported from the `sql_queries.py` module that is discussed below. The `create_tables.py` then defines the top-level module `__main__` for the script, essentially denoting what functions to execute if the `create_tables.py` is called. It executes the previously defined function in order and then closes the connection.

    def main():
        cur, conn = create_database()
    
        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()

The `__main__` modules is only executed if the name of the called script matches the name of the script, thus this conditional is added to check that relation. 

    if __name__ == "__main__":
        main()
        
For the `create_tables(cur, conn)` and `drop_tables(cur, conn)` functions in `create_tables.py`, imported variables `drop_table_queries` and `create_table_queries` are called. These variables are defined in the script `sql_queries.py`. The `drop_table_queries` are a series of Postgres queries that drop the tables defined in the Schema section above. 

    songplay_table_drop = "DROP TABLE IF EXISTS songplays"
    user_table_drop = "DROP TABLE IF EXISTS users"
    song_table_drop = "DROP TABLE IF EXISTS songs"
    artist_table_drop = "DROP TABLE IF EXISTS artists"
    time_table_drop = "DROP TABLE IF EXISTS time"
    
    drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

The `create_table_queries` are a series of Postgres queries to create the tables defined in the Schema section above.

    songplay_table_create = ("CREATE TABLE IF NOT EXISTS \
                              songplays ( \
                              start_time VARCHAR \
                            , level TEXT \
                            , song_id VARCHAR \
                            , artist_id VARCHAR \
                            , session_id INT \
                            , location VARCHAR \
                            , user_agent VARCHAR \
                            , PRIMARY KEY (start_time))")
    
    user_table_create = ("CREATE TABLE IF NOT EXISTS \
                          users ( \
                          user_id INT \
                        , first_name TEXT \
                        , last_name TEXT \
                        , gender TEXT \
                        , level TEXT \
                        , PRIMARY KEY (user_id))")
    
    song_table_create = ("CREATE TABLE IF NOT EXISTS \
                          songs ( \
                          song_id VARCHAR \
                        , title VARCHAR \
                        , artist_id VARCHAR \
                        , year INT \
                        , duration REAL \
                        , PRIMARY KEY (song_id))")
    
    artist_table_create = ("CREATE TABLE IF NOT EXISTS \
                            artists ( \
                            artist_id VARCHAR \
                          , name VARCHAR \
                          , location VARCHAR \
                          , latitude NUMERIC \
                          , longitude NUMERIC \
                          , PRIMARY KEY (artist_id))")
    
    time_table_create = ("CREATE TABLE IF NOT EXISTS \
                          time ( \
                          start_time VARCHAR \
                        , hour INT \
                        , day INT \
                        , week INT \
                        , month INT \
                        , year INT \
                        , weekday TEXT )")

    create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

The script `sql_queries.py` includes other queries used elsewhere in the project and are discussed later. The process of creating the database and tables involves simply executing the `create_tables.py` script from the terminal:

    python create_tables.py
    
####Processing the Datasets
A script called `etl.py` is used to execute the functions that extract, transform, and load the song data into the songs and artist tables. It imports `psycopg2`, `os`, `pandas` for data processing, `glob` for finding filepaths, and the variables from script `sql_queries.py`, which will be discussed later. The first part of the script `etl.py` to discuss is the last section. The `__main__` module uses `psycopg2` to connect to the database and generatea a cursor. Then two `process_data()` functions are executed, which are discussed later. Finally, the connection is closed. Since the name/main conditional is added, the `__main__` module will execute if the `etl.py` script is called in the terminal.

    def main():
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    
        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
        conn.close()
    
    if __name__ == "__main__":
        main()

The `__main__` module executes two `process_data()` functions. The `process_data()` function takes four arguments: The cursor, the connection object, the filepath to start at, and the sub-function to execute. The `process_data()` function first generates a list `all_files` of all JSON files under the input filepath using the `os` and `glob` modules. Then the function iterates through the list, executing the input sub-function for all the files in the `all_files` list.

    def process_data(cur, conn, filepath, func):
        # get all files matching extension from directory
        all_files = []
        for root, dirs, files in os.walk(filepath):
            files = glob.glob(os.path.join(root,'*.json'))
            for f in files :
                all_files.append(os.path.abspath(f))
    
        # get total number of files found
        num_files = len(all_files)
        print('{} files found in {}'.format(num_files, filepath))
    
        # iterate over files and process
        for i, datafile in enumerate(all_files, 1):
            func(cur, datafile)
            conn.commit()
            print('{}/{} files processed.'.format(i, num_files))

####Processing the Songs Dataset
The `__main__` module of the script `etl.py` calls the `process_data()` function for the `data/song_data` filepath and `process_song_file` sub-function. The function `process_song_file()` is defined earlier in the `etl.py` script. The function extracts data from one of the songs data JSON files. The songs data files contain data needed for songs and artists dimension tables. The function uses `pandas` to read the input JSON file and generates a dataframe. This extracts all columns from the JSON file, but only a select few columns are needed for the songs and artists dimension tables. Thus a new variable, unique to the songs and artists tables, is generated that 1) selects only the dataframe columns of interest, 2) specifically extracts only the values from the trimmed down dataframe, and finally converts the values into a list. Then the `song_table_insert` and `artist_table_insert` variables, imported from the `sql_queries.py` script, are executed using `psycopg2`. These are Postgres table insert commands defined in `sql_queries.py`.

    def process_song_file(cur, filepath):
        # open song file
        df = pd.read_json(filepath, lines=True)
    
        # insert song record
        song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
        # selects song columns from df, extracts only the values, and converts it to a list
        cur.execute(song_table_insert, song_data)
    
        # insert artist record
        artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
        # selects artist columns from df, extracts only the values, and converts it to a list
        cur.execute(artist_table_insert, artist_data)

The `song_table_insert` and `artist_table_insert` variables are defined the script `sql_queries.py`. The variables define Postgres table insert commands with placeholder values variables that reference back to the table specific data lists `song_data` and `artist_data` in the script `etl.py`. If there is an attempt to insert a song or artist that already is in the table (conflict on primary key), no new entry is inserted.

    song_table_insert = ("INSERT INTO songs (song_id, title, artist_id, year, duration) \
                          VALUES (%s, %s, %s, %s, %s) \
                          ON CONFLICT (song_id) DO NOTHING;")
    
    artist_table_insert = ("INSERT INTO artists (artist_id, name, location, latitude, longitude) \
                            VALUES (%s, %s, %s, %s, %s) \
                            ON CONFLICT (artist_id) DO NOTHING")

####Processing the Log Dataset
The `__main__` module of the script `etl.py` calls the `process_data()` function for the `data/log_data` filepath and `process_log_file` sub-function. The function `process_log_file()` is defined earlier in the `etl.py` script. The function extracts data from one of the log data JSON files. The log data files contain data needed for the users and time dimension tables as well as the songplays fact table. The function uses `pandas` to read the input JSON file and generates a dataframe. This extracts all columns from the JSON file, but only selects for rows where the page is NextSong. 

    df = pd.read_json(filepath, lines=True)
    df = df[df.page == 'NextSong']

For the time dimension table, first the timestamp value from the df dataframe is converted to datetime using the `to_datetime` module of pandas and saved to a new variable. From this variable, the time dimension table is constructed.
 
    t = pd.to_datetime(df.ts, unit='ms')
     
Next a set of series are generated from the datetime value and converted to the value of the columns needed in the time dimension table.

    time_data_time = t.dt.time.values.tolist()
    time_data_hour = t.dt.hour.values.tolist()
    time_data_day = t.dt.day.values.tolist()
    time_data_week = t.dt.week.values.tolist()
    time_data_month = t.dt.month.values.tolist()
    time_data_year = t.dt.year.values.tolist()
    time_data_weekday = t.dt.weekday.values.tolist()
    
Then a dataframe needs to be created, populated with the data from previously defined time series. To do this first an empty time_data list is defined and then each time series is iterated through by index and added to a time_entry row list. This time_entry list is then added to the time_data list. This is repeated for each row in the set of the series until an array (list of lists) is generated. A column label list is defined, corresponding to the entries in each row. These two components, the time_data array and column label list, are used to generate the time_df dataframe. 

    time_data = []
    for n in range(len(t)):
        time_entry =[]
        time_entry.append(time_data_time[n])
        time_entry.append(time_data_hour[n])
        time_entry.append(time_data_day[n])
        time_entry.append(time_data_week[n])
        time_entry.append(time_data_month[n])
        time_entry.append(time_data_year[n])
        time_entry.append(time_data_weekday[n])
        time_data.append(time_entry)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(data=time_data, columns=column_labels)

Then each row of the dataframe is iterated through to generate a list of the time values in each row. This row list is used as the injected values in the Postgres INSERT expression `time_table_insert` defined in the `sql_queries.py` script, which is repeated for each row, adding all the time data to the time dimension table. If there is an attempt to insert a time entry that already is in the table (conflict on primary key), no new entry is inserted.

    time_table_insert = ("INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                          VALUES (%s, %s, %s, %s, %s, %s, %s) \
                          ON CONFLICT (start_time) DO NOTHING;")

For the users dimension table a user_df dataframe is generated from the log dataframe df. The user_df selects only the columns applicable to the users dimension table. Then each row of the user_df dataframe is iterated through, inserting the row's values into the users dimension table.

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
The `user_table_insert` variable is defined in the `sql_queries.py` script.

    user_table_insert = ("INSERT INTO users (user_id, first_name, last_name, gender, level) \
                          VALUES (%s, %s, %s, %s, %s) \
                            ON CONFLICT (user_id) DO NOTHING;") # Added due to duplicate users
                            
For the songplays fact table