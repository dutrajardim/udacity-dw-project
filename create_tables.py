import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: This function is responsible for deleting all
    tables by executing all queries in the 'drop_table_queries' list.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.

    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: This function is responsible for creating all
    by executing all queries in the 'create_table_queries' list.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.

    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function is responsible for creating
    the data warehouse with all data definitions needed. If the DWH
    already exists, it is deleted before creating it.

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

    # executing data definition queries
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
