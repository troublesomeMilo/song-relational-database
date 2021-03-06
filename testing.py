import psycopg2
import pandas as pd

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=password1")
cur = conn.cursor()

cur.execute("""SELECT * FROM songs""")
results_songs = cur.fetchall()
print("SONGS TABLE TEST")
print("Labels: song_id, title, artist_id, year, duration")
for result in results_songs:
    print(result)
	
cur.execute("""SELECT * FROM artists""")
results_artists = cur.fetchall()
print("ARTISTS TABLE TEST")
print("Labels: artist_id, name, location, lattitude, longitude")
for result in results_artists:
    print(result)

cur.execute("""SELECT * FROM users LIMIT 5""")
results_users = cur.fetchmany(5)
print("USERS TABLE TEST")
print("Labels: user_id, first_name, last_name, gender, level")
for result in results_users:
    print(result)

cur.execute("""SELECT * FROM time LIMIT 5""")
results_time = cur.fetchmany(5)
print("TIME TABLE TEST")
print("Labels: datetime, hour, day, week, month, year, weekday")
for result in results_time:
    print(result)
	
cur.execute("""SELECT * FROM songplays WHERE song_id <> 'None'; """)
results_select = cur.fetchone()
print("SELECT TEST")
for result in results_select:
    print(result)
	
cur.execute("""SELECT * FROM songplays LIMIT 50""")
results_songplays = cur.fetchmany(50)
print("SONGPLAYS TABLE TEST")
print("Labels: starttime, user_id, level, song_id, artist_id, session, location, userAgent")
for result in results_songplays:
    print(result)
	
conn.close()