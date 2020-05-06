############################################################################################################
#             CHANGE LOG
# This script extracts the songs and log objects from S3 buckets  loads in to staging tables and using the 
# staging table insert the data in to 
# Dimensions (songs, artists, users , time ) and Fact (songsplay) table
#
# Below outlines the flow of the script.
# 1) import the required libraries and sqlqueries from sql_queries script 
# 2) Create a database connection  
# 3) Create a cursor using the connection
# 4) Copy the songs object in to staging table staging_songs and copy the events to staging_events table
# 5) Insert dimensional tables users and time from staging_events table
# 6) Insert dimensional tables songs and artists from staing_songs table
# 7) Insert songsplay fact table from  staging_events where page is 'NextSong' and song_id , artist id from staging_songs table.
# 8) Do a quality check to see no of records in tables are correct.
# 9) close the connection of the database
############################################################################################################
import configparser
import psycopg2
from datetime import datetime
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ load stage tables from S3 bucket"""
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(" Exception in loading stage tables using" + str(query) + "and the error is: " + str(e))

def insert_tables(cur, conn):
    """ insert the records from staging tables to fact and dimension tables"""
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(" Exception in insert  tables using" + str(query) + "and the error is: " + str(e))
            

def main():
    """ main fucntion to connect the database, create the cursor , load staging tables from S3 bucket
    and load the fact , dimension tables , check the records count and close the database connection"""
    try:
        # create config parser and read the config file
        config = configparser.ConfigParser()
        config.read('dwh.cfg')
        # connect to the cluster data warehouse
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        # load the staging tables and connect to the cluster data warehouse
        try:
            load_staging_tables(cur, conn)
        except Exception as e:
            print(" Exception in loading stage tables so no loading the fact and dimensional tables \
                  and the error is: " + str(e))        
        else:
            # excute the insert tables function only when the loading of stage tables is successful
            insert_tables(cur, conn)

        conn.close()
    except Exception as e:
        print(" Exception in main fucntion and the error is: " + str(e))        


if __name__ == "__main__":
    print('start of the etl script :' + str(datetime.now()))
    # excute the main function
    main()
    print('end of the etl script :' + str(datetime.now()))