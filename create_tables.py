import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop tables if any by executing DROP queries.
    DROP queries are defined in drop_table_queries.

    Args:
        cur: SQL cursor object.
        conn: DB connection object.
    Returns:
    Raises:
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create tables by executing CREATE queries.
    CREATE queries are defined in create_table_queries.

    Args:
        cur: SQL cursor object.
        conn: DB connection object.
    Returns:
    Raises:
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")
    print("connecting to RDS...")
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()
    print("DROP EXISTING TABLES...")
    drop_tables(cur, conn)
    print("CREATE TABLES...")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
