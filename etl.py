import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    this function accesses the song and log datasets and extracts the columns which are important for the analysis
    for each file the available data is inserted into the staging table
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    this function accesses the staging tables and uses these tables to insert into the final tables (users, artists, songs, time, songplays)
    for each file the available data is inserted into the staging table
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    this function connects to the the cluster and redshift database and executes the functions to access data from the files and insert it into staging tables and finally the "final tables"
    connection is closed to the database
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()