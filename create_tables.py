import configparser
from logging import getLogger
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

logger = getLogger(__name__)


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")
    logger.info("connecting to RDS...")
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()
    logger.info("DROP EXISTING TABLES...")
    drop_tables(cur, conn)
    logger.info("CREATE TABLES...")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
