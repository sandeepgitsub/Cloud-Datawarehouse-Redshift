############################################################################################################
#             CHANGE LOG
# This script creates the staging tables to store the data from S3 and also creates
# Dimenstional (songs, artists, users , time ) and Fact (songsplay) tables
# It uses all the queries from sql_queries.py module
#
# Below outlines the flow of the script.
# 1) import the required libraries and sqlqueries from sql_queries script 
# 2) Create a database connection  
# 3) Create a cursor using the connection
# 4) drop the table if exists
# 5) create the tables
# 6) close the database connection.
############################################################################################################

import configparser
import psycopg2
from datetime import datetime
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ drop the table using drop table list"""
    
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(" dropping of table using " + query + " not successful")
            print(e)

        

def create_tables(cur, conn):
    """ create the table using create table list """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(" creating table using " + query + " not successful")
            print(e)

def main():
    """ create the cur and connection , drop the table if exists ,create the tables  and close the connection"""
    try:
        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()
    except Exception as e:
        print(" exception in main script and the error is " + str(e))


if __name__ == "__main__":
    print('start of the create tables script :' + str(datetime.now()))
    # execute the main function
    main()
    print('end of the create tables script :' + str(datetime.now()))