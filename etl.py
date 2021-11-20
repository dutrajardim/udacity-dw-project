import configparser
import psycopg2
import logging
from sql_queries import (
    copy_table_queries,
    insert_table_queries,
    create_staging_table_queries,
)


def load_staging_tables(cur, conn):
    """
    Description: This function is responsible for loading JSON files
    to staging tables.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.

    Returns:
        None
    """
    logging.info("Loading data to staging tables.")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def create_staging_tables(cur, conn):
    """
    Description: This function is responsible for creating
    the temporary tables for loading files. It will be
    auto-deleted at the end of the session.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.

    Returns:
        None
    """
    logging.info("Creating staging tables.")
    for query in create_staging_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: This function is responsible for transforming the
    data in staging tables and loading it in the star schema tables.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.

    Returns:
        None
    """
    logging.info("Inserting data to tables.")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function is responsible for executing the transformations and
    the ingest process.

    Arguments:
        None

    Returns:
        None
    """

    # loading configurations
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    # connecting to redshift
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config["DB"].values())
    )
    cur = conn.cursor()

    # creating temporary tables
    create_staging_tables(cur, conn)

    # loading data
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    # set logging
    logging.root.setLevel(logging.INFO)
    
    main()
