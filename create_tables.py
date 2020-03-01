from typing import List, Tuple

import psycopg2
from psycopg2.extensions import connection, cursor

from db_values import HOST, DBNAME, USER, PASSWORD
from sql_queries import CREATE_TABLE_QUERIES, DROP_TABLE_QUERIES


def create_database(host: str = HOST,
                    user: str = USER,
                    password: str = PASSWORD,
                    dbname: str = DBNAME
                    ) -> Tuple[cursor, connection]:
    """
    Create user `user` in the default PostgreSQL database and create fresh
    database `dbname` as user `user`.

    Parameters
    ----------
    host : str, optional
        Host name.
    user : str, optional
        User name.
    password : str, optional
        Password.
    dbname : str, optional
        Database name.

    Returns
    -------
    psycopg2.extensions.cursor, psycopg2.extensions.connection
        Database cursor and connection to `dbname`.
    """
    # Connect to default PostgreSQL database, create user, close connection
    DEFAULT_DBNAME = 'postgres'
    conn = psycopg2.connect(host=host, dbname=DEFAULT_DBNAME)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    try:
        cur.execute(f'CREATE ROLE {user:s} WITH LOGIN CREATEDB '
                    f'PASSWORD \'{password:s}\';')
    except psycopg2.Error as e:
        # Probably role already exists
        # print(e)
        pass
    finally:
        conn.close()

    # Connect to default PostgreSQL database as the new user
    conn = psycopg2.connect(host=host, dbname=DEFAULT_DBNAME, user=user,
                            password=password)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Create fresh `dbname` database with UTF-8 encoding
    cur.execute(f'DROP DATABASE IF EXISTS {dbname:s};')
    cur.execute(f'CREATE DATABASE {dbname:s} WITH ENCODING "UTF8" '
                f'TEMPLATE template0;')

    # Close connection to default database
    conn.close()

    # Connect to the new database
    conn = psycopg2.connect(host=host, dbname=dbname, user=user,
                            password=password)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return cur, conn


def execute_queries(cur: cursor, queries: List[str]) -> None:
    """
    Execute list of queries in database.

    Parameters
    ----------
    cur : psycopg2.extensions.cursor
        Database cursor.
    queries : list[str]
        List of queries to run.

    Returns
    -------
    None
    """
    for query in queries:
        cur.execute(query)


def main():
    cur, conn = create_database()

    execute_queries(cur, queries=DROP_TABLE_QUERIES)
    execute_queries(cur, CREATE_TABLE_QUERIES)
    print('All tables were successfully created!')

    conn.close()


if __name__ == "__main__":
    main()
