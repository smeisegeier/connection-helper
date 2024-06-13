import os
import sqlite3
from typing import Literal
import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy_utils import create_database, database_exists


def connect_sql(
    db: str,
    host: str = "",
    user: str = "",
    pw: str = "",
    dbms: Literal["mssql", "sqlite", "postgres"] = "mssql",
    ensure_db_exists: bool = False,
) -> object:
    """
    Connects to any SQL database based on the given parameters.

    Args:
        db (str): The name of the database / sqlite-file.
        host (str, optional): The host name or IP address of the database server. Defaults to an empty string.
        user (str, optional): The username for authentication. Defaults to an empty string.
        pw (str, optional): The password for authentication. Defaults to an empty string.
        dbms (Literal['mssql', 'sqlite','postgres'], optional): The type of database management system. Defaults to 'mssql'.
        ensure_db_exists (bool, optional): Specifies whether to create the database if it does not exist. Defaults to False.

    Returns:
        Connection: The context object for the established database connection

    Remarks:
        - postgres
            - psycopg2-binary must be installed
            - example:
                con = my_connect_to_any_sql(
                    host='<instance>.postgres.database.azure.com',
                    db='eteste',
                    user='<user>n@<instance>',
                    pw='<password>',
                    dbms='postgres',
                    ensure_db_exists=False
                )

    """

    if dbms == "mssql":
        url = f"mssql://{user}:{pw}@{host}/{db}?driver=ODBC Driver 17 for SQL Server"
    elif dbms == "sqlite":
        url = f"sqlite:///{db}"
    elif dbms == "postgres":
        url = f"postgresql+psycopg2://{user}:{pw}@{host}/{db}"
    else:
        print("dbms not supported")
        return None

    engine = create_engine(
        url,  # * leave positional argument unnamed, since it was relabeled between versions..
        connect_args={"connect_timeout": 10},
    )

    # * ensure db exists
    if ensure_db_exists:
        if not database_exists(engine.url):
            create_database(engine.url)
            print(f"db {db} created")
        else:
            print(f"db {db} exists")

    # * now connect
    try:
        con = engine.connect()
    except Exception as e:
        print(e)

    return con


def unpack_sqlite_to_parquet(
    file_sqlite: str,
    dir_local: str,
    fetch_views: bool = False,
    where_clause: str = "",
    verbose: bool = False,
) -> None:
    """
    Unpacks a SQLite database file into individual Parquet files for each table.

    Args:
        file_sqlite (str): The path to the SQLite database file.
        dir_local (str): The directory where the Parquet files will be saved.
        fetch_views (bool, optional): Whether to include views in the unpacking. Defaults to False.
        where_clause (str, optional): The optional WHERE clause to filter the tables. Defaults to "". Statement starts after the WHERE keyword.
        verbose (bool, optional): Whether to print the progress. Defaults to False.

    Returns:
        None
    """
    if not os.path.exists(dir_local):
        os.makedirs(dir_local)

    views = ",'view'" if fetch_views else ""
    filter=f" AND {where_clause}" if where_clause else ""
    qry = f"SELECT name,sql FROM sqlite_master WHERE type in ('table'{views}) AND name NOT LIKE 'sqlite_%'{filter};"

    con_sqlite = sqlite3.connect(file_sqlite)
    with con_sqlite:
        df_tables = pd.read_sql(con=con_sqlite, sql=qry)

    # * write tables in a loop
    for tbl in df_tables["name"].to_list():
        df = pd.read_sql(con=con_sqlite, sql=f"SELECT * FROM {tbl}")
        path = os.path.join(dir_local, f"{tbl}.parquet")
        if verbose:
            print(f"⏳ writing: {path} with {df.shape}")
        df.to_parquet(path,index=False)

    con_sqlite.close()
