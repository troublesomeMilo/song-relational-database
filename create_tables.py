import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
	Connects to the database, generates a cursor, drops existing db, 
	creates a new db, disconnects, reconnects and generates a new cursor.
	
	Arguments: None
	Returns: cur=cursor object, conn=connection object
	"""
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=test user=postgres password=password1")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=password1")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
	Executes the set of queries stored in drop_table_queries to drop existing tables.
	
	Arguments: cur=cursor object, conn=connection object
	Returns: None
	"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
	Executes the set of queries stored in create_table_queries to create new tables.
	
	Arguments: cur=cursor object, conn=connection object
	Returns: None
	"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
	When the script is directly called, the main method runs the create_database() function, 
    drop_tables() function, and create_tables() function, and then disconnects.
	
	Arguments: None
	Returns: None
	"""
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()