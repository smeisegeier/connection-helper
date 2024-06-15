import os
import sqlite3
from typing import Literal
import pandas as pd
import duckdb as ddb

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
    overwrite: bool = False,
    verbose: bool = True,
    debug: bool = False,
) -> None:
    """
    Unpacks a SQLite database file into individual Parquet files for each table.

    Args:
        file_sqlite (str): The path to the SQLite database file.
        dir_local (str): The directory where the Parquet files will be saved.
        fetch_views (bool, optional): Whether to include views in the unpacking. Defaults to False.
        where_clause (str, optional): The optional WHERE clause to filter the tables. Defaults to "". Statement starts after the WHERE keyword.
        overwrite (bool, optional): Whether to overwrite existing files. Defaults to False.
        verbose (bool, optional): Whether to print the progress. Defaults to True.
        debug (bool, optional): Whether to debug. Defaults to False.

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
        
    if debug:
        print("🧪 debugging 🧪")

    # * write tables in a loop
    for tbl in df_tables["name"].to_list():
        path = os.path.join(dir_local, f"{tbl}.parquet")        
        exists = os.path.exists(path)
        df = None

        if not debug and ((overwrite and exists) or (not exists)):
            df = pd.read_sql(con=con_sqlite, sql=f"SELECT * FROM {tbl}")

        shape = df.shape if df is not None else "???" 

        if verbose or debug:
            if exists:
                if overwrite:
                    print(f"⏳ replacing: {path} ➡️ {shape}")
                else:
                    print(f"💨 skipping: {path}")
            else:
                print(f"⏳ creating: {path} ➡️ {shape}")
        if not debug and ((overwrite and exists) or (not exists)):
            df.to_parquet(path,index=False)

    con_sqlite.close()

def unpack_files_to_duckdb(
    dir: Path,
    ext: Literal["csv", "parquet"],
    list_files: list[str] = None,
    prefix: str = "",
    verbose: bool = False,
    debug: bool = False,
):
    """
    Unpacks files from a given directory to a DuckDB database.

    Args:
        dir (Path): The directory containing the files to unpack.
        ext (Literal["csv", "parquet"]): The file extension to unpack.
        list_files (list[str], optional): A list of files to unpack. Defaults to None.
        prefix (str, optional): A prefix to add to the unpacked files. Defaults to "".
        verbose (bool, optional): Whether to print loading messages. Defaults to False.
        debug (bool, optional): Whether to return a string instead of a DuckDB database. Defaults to False.

    Returns:
        Union[Tuple, str]: If debug is False, returns a tuple of DuckDB tables. If debug is True, returns a string of sorted file names.

    """
    files = set([os.path.basename(file).split(".")[0] for file in os.listdir(dir)])
    
    # * filter files if present
    if list_files is not None:
        files = files & set(list_files)
    
    # * exit or sort
    if files is None:
        return None
    else: files=sorted(files)

    items = []
    for file in files:
        if verbose:
            print(f"⏳ loading {file}")
        if not debug:
            if ext == "parquet":
                items.append(
                    ddb.read_parquet((dir / f"{file}.parquet").as_posix())
                )
            elif ext == "csv":
                items.append(
                    ddb.read_csv((dir / f"{file}.csv").as_posix(), header=True)
                )

    if not debug:
        # * unpacking the ddb files in tupel notation works
        out = (*items,)
    else:
        files = [f"{prefix}{file}" for file in files]
        out = str(sorted(files)).replace("'", "").replace("[", "").replace("]", "")

    return out