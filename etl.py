import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Extract JSON files into staging tables by executing COPY queries.
    COPY queries are defined in copy_table_queries.

    Args:
        cur: SQL cursor object.
        conn: DB connection object.
    Returns:
    Raises:
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Convert staging tables into star schema by executing INSERT queries.
    INSERT queries are defined in insert_table_queries.

    Args:
        cur: SQL cursor object.
        conn: DB connection object.
    Returns:
    Raises:
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()
    # NOTE: COPY query may be timeout when thrown from other region.
    # https://docs.aws.amazon.com/redshift/latest/mgmt/connecting-firewall-guidance.html
    print('Building staging tables.')
    load_staging_tables(cur, conn)
    print('Convert staging tables into star schema.')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
