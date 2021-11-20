import configparser
import psycopg2
from sql_queries import (
    copy_table_queries,
    insert_table_queries,
    create_staging_table_queries,
)


def load_staging_tables(cur, conn):
    print("Loading data to staging tables.")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def create_staging_tables(cur, conn):
    print("Creating staging tables.")
    for query in create_staging_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    print("Inserting data to tables.")
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config["DB"].values())
    )
    cur = conn.cursor()

    create_staging_tables(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
