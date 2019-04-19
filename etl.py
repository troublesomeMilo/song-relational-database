import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
	Processes a song data file. Reads the JSON file at the filepath location to a dataframe. 
	This funciton populates the users and artist dimension tables. 
	Selects data from the dataframe related to song information and writes it to a list as 
	values only. An imported INSERT query is executed to insert the song data from the 
	given file into the songs dimension table. Then selects data from the dataframe related 
	to artist information and writes it to a list as values only. An imported INSERT query is 
	executed to insert artist data from the given file into the artists dimension table.
	
	Arguments: cur=cursor object, filepath=filepath to data file
	Returns: None
	"""
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    # selects song columns from df, extracts only the values, and converts it to a list
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 
                      'artist_latitude', 'artist_longitude']].values[0].tolist()
    # selects artist columns from df, extracts only the values, and converts it to a list
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
	Processes a log data file. Reads the JSON file at the filepath location to a dataframe. Selects
	only the rows where the page column is NextSong. This function populates the time and users
	dimension tables and songplays fact table. 
	
	Arguments: cur=cursor object, filepath=filepath to data file
	Returns: None
	"""
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']
	
    """
	The starttime values are converted to a datetime series. From this series new series are generated 
	for each of the time increments of the time dimension table. The individual series are converted to 
	value-only lists. Then the lists are iterated through to generate an array of time values. This array
	is used to generate a dataframe of time values. Then the dataframe is iterated through so each row
	of the dataframe is inserted into the time dimension table using an imported query. 
	"""

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records 
    # generate the individual time value series
    time_data_time = t.dt.time.values.tolist()
    time_data_hour = t.dt.hour.values.tolist()
    time_data_day = t.dt.day.values.tolist()
    time_data_week = t.dt.week.values.tolist()
    time_data_month = t.dt.month.values.tolist()
    time_data_year = t.dt.year.values.tolist()
    time_data_weekday = t.dt.weekday.values.tolist()
    # generate the time_data array be appending row value lists
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

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    """
	The data applicable to the user is selected for from the dataframe. This user dataframe is 
	iterated through, inserting each row into the users dimension table using an imported query. 
	"""

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    """
	The log data does not contain song_id or artist_id needed for the songplays fact table. To 
	obtain this data, the song name and artist name from the log dataframe is injected into an 
	imported SELECT query that returns the song_id and artist_id for songs with matching 
	song names and artist names. The song_id and artist_id are inserted, along with other 
	applicable song play information into the songplays fact table. 
	"""

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, 
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
	Generic data process function. The directories in the file defined by filepath are searched 
	for JSON files to generate a list of filepaths to each data file. The total number of files found 
	is printed. Then the list of filepaths is iterated through, executing the argument func function 
	on each filepath.
	
	Arguments: cur=cursor object, conn=connection object, filepath=file path of root directory
	of data files, func=the data type specific (song vs log) processing function
	Returns: None
	"""
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


def main():
    """
	When the script is directly called, the main method connects and generates a cursor
	and runs the process_data() function for both the song data and log data by calling
	the process_song_file() and process_log_file() functions, respectively. Finally, the 
	connectio is closed.
	
	Arguments: None
	Returns: None
	"""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=password1")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()