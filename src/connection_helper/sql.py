import os
from os.path import expanduser

import sqlite3
from urllib.parse import urlparse
import pandas as pd
import duckdb as ddb
from pathlib import Path
import datetime as dt

from sqlalchemy import create_engine, text
import sqlalchemy.engine
from sqlalchemy_utils import create_database, database_exists

from typing import List, Optional, Union, Literal

from dotenv import load_dotenv, find_dotenv


def is_url(url: str) -> bool:
    parsed = urlparse(url)
    return all([parsed.scheme, parsed.netloc])


def connect_sql(
    db: str = "",
    host: str = "",
    user: str = "",
    pw: str = "",
    use_env: bool = True,
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
        use_env (bool, optional): Whether to use environment variables for the connection. Defaults to True. If provided, host, db, user, pw will be ignored. Example: If there is an item "SQL_HOST" in .env, use: (host = "SQL_HOST", use_env = True)
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

    if use_env:
        # Load environment variables
        load_dotenv(find_dotenv())
        host = os.getenv(host)
        db = os.getenv(db)
        # ! a "" str will be ignored in conn str, a None str wont
        if user:
            user = os.getenv(user)
        if pw:
            pw = os.getenv(pw)

    # Ensure host and db are provided
    if not host or not db:
        raise ValueError(
            "❌ Both host and db must be provided either through environment variables or directly as arguments."
        )

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
    con = None
    try:
        con = engine.connect()
    except Exception as e:
        print(e)

    return con


def load_sql_to_sqlite(
    con_source,
    file_db: str,
    list_tables: list[str],
    dict_meta: dict = None,
    dict_views: dict = None,
    top_n_rows: int = 0,
    verbose: bool = True,
) -> None:
    """
    Load SQL tables into a SQLite database. Adds table _meta with metadata if dict_meta is given. Adds views if dict_views is given.

    Args:
        con_source (object): The connection object to the source database.
        file_db (str): The path to the SQLite database file.
        list_tables (list[str]): A list of SQL tables to load. Each table can be specified as a string or a list of two strings, where the first string is the table name in the source database and the second string is the table name in the SQLite database (optional).
        dict_meta (dict, optional): A dictionary containing metadata to be written to the SQLite database. Defaults to None.
        dict_views (dict, optional): A dictionary containing views to be written to the SQLite database. Defaults to None. Structure: {view_name: view_query}. view_query must not include the create view statement, but only the select statement.
        top_n_rows (int, optional): The number of rows to load from each table. Defaults to 0.
        verbose (bool, optional): Whether to print progress messages. Defaults to True.

    Returns:
        None

    Raises:
        None

    Description:
        This function loads SQL tables from a source database into a SQLite database. If the SQLite database file already exists, the function exits. The function writes metadata to the SQLite database if a dictionary is provided. The function loads tables in batches of 10,000 rows and appends the loaded data to the corresponding table in the SQLite database.
    """
    # * check if db already exists
    if os.path.exists(file_db):
        print(f"❌ {file_db} already exists. exiting..")
        return

    con_sqlite = sqlite3.connect(file_db)
    batchsize = 10000

    # * create views
    cursor = con_sqlite.cursor()
    if dict_views is not None:
        for key, value in dict_views.items():
            cursor.execute(
                f"create view if not exists {key} as {value.replace(';','')};"
            )

    # * write meta table if dict was given
    if dict_meta is not None:
        df_meta = pd.DataFrame.from_dict(dict_meta, orient="index").T
        df_meta.to_sql("_meta", con_sqlite, if_exists="replace", index=False)

    # * check if list is nested
    is_list_nested = all([isinstance(i, list) for i in list_tables])

    for item in list_tables:
        if is_list_nested:
            table_sql = item[0]
            table_friendly = (
                item[1]
                if item[1]
                else item[0] if "." not in item[0] else item[0].split(".")[1]
            )
        else:
            table_sql = item
            table_friendly = item if "." not in item else item.split(".")[1]

        if verbose:
            print(f"processing: {table_sql} -> {table_friendly}")

        top = f" top {top_n_rows}" if top_n_rows else ""
        qry = f"select{top} * from {table_sql}"

        proxy = con_source.execution_options(stream_results=True).execute(text(qry))
        cols = list(proxy.keys())

        # todo pandas -> duckdb ??
        while "batch not empty":
            batch = proxy.fetchmany(batchsize)
            df = pd.DataFrame(batch, columns=cols)
            df.to_sql(table_friendly, con_sqlite, if_exists="append", index=False)
            if not batch:
                break

    con_sqlite.close()
    return

# todo handle path arguments in single logic
def load_sqlite_to_parquet(
    file_sqlite: str,
    dir_local: str,
    fetch_views: bool = False,
    table_filter: str = "",
    overwrite: bool = False,
    verbose: bool = True,
    debug: bool = False,
) -> None:
    """
    Saves tables of a SQLite database file into individual Parquet files for each table.
    This method uses duckdb engine.

    Args:
        file_sqlite (str): The path to the SQLite database file.
        dir_local (str): The directory where the Parquet files will be saved.
        fetch_views (bool, optional): Whether to include views in the unpacking. Defaults to False.
        tables_filter (str, optional): The optional filter to apply to the table names. Defaults to "". Is a pandas filter query, example: "table_name.str[0] == '_'"
        overwrite (bool, optional): Whether to overwrite existing files. Defaults to False.
        verbose (bool, optional): Whether to print the progress. Defaults to True.
        debug (bool, optional): Whether to debug. Defaults to False.

    Returns:
        None
    """

    if not os.path.exists(dir_local):
        os.makedirs(dir_local)

    # todo add support for views
    # views = ",'view'" if fetch_views else ""

    # * retrieve db name from file, this will be default name
    db_name = os.path.basename(file_sqlite).split(".")[0]

    # * connect to db
    con = ddb.connect(file_sqlite, read_only=True)

    # * retrieve all tables. this cant be filtered by database_name (weird effect)
    df_db = con.sql("SELECT * FROM duckdb_tables();").to_df()

    # * narrow down tables
    df_tbl = df_db.query(table_filter) if table_filter else df_db

    if debug:
        print("🧪 debugging 🧪")
        # display(df_tbl)

    # * write tables in a loop
    for tbl in df_tbl["table_name"]:
        path = os.path.join(dir_local, f"{tbl}.parquet")
        exists = os.path.exists(path)

        if verbose or debug:
            if exists:
                if overwrite:
                    print(f"⏳ replacing: {path}")
                else:
                    print(f"💨 skipping: {path}")
            else:
                print(f"⏳ creating: {path}")

        if not debug and ((overwrite and exists) or (not exists)):
            # todo add top_n_rows
            qry = f"copy (select * from {tbl}) to '{path}'"
            con.sql(qry)

    con.close()
    return


def unpack_files_to_duckdb(
    dir: Path,
    ext: Literal["csv", "parquet"],
    con=None,
    list_files: list[str] = None,
    prefix: str = "",
    verbose: bool = False,
    debug: bool = False,
):
    """
    Unpack files from a given directory to a DuckDB database (['csv', 'parquet']).
    Can use an existing DuckDB connection to bundle all relations.

    Args:
        dir (Path): The directory containing the files to unpack.
        ext (Literal["csv", "parquet"]): The file extension to unpack.
        con (duckdb connection, optional): The DuckDB connection to use. Defaults to None.
        list_files (list[str], optional): A list of files to unpack. Defaults to None.
        prefix (str, optional): A prefix to add to the unpacked files. Defaults to "".
        verbose (bool, optional): Whether to print loading messages. Defaults to False.
        debug (bool, optional): Whether to return a string instead of a DuckDB database. Defaults to False.

    Returns:
        Union[Tuple, str]: If debug is False, returns a tuple of DuckDB tables. If debug is True, returns a string of sorted file names.

    """
    # * if no con given, create new
    con_ = con if con else ddb.connect()

    # * get basename of each file
    files = set([os.path.basename(file).split(".")[0] for file in os.listdir(dir)])

    # * filter files if present
    if list_files is not None:
        files = files & set(list_files)

    # * exit or sort
    if files is None:
        return None
    else:
        files = sorted(files)

    items = []
    for file in files:
        if verbose or debug:
            print(f"⏳ loading {file}")
        if not debug:
            if ext == "parquet":
                items.append(con_.read_parquet((dir / f"{file}.parquet").as_posix()))
            elif ext == "csv":
                items.append(
                    con_.read_csv((dir / f"{file}.csv").as_posix(), header=True)
                )

    if not debug:
        # * unpacking the ddb files in tupel notation works
        # * tuple trick seems to not work on 1-item list
        out = items[0] if len(items) == 1 else (*items,)
    else:
        files = [f"{prefix}{file}" for file in files]
        out = str(sorted(files)).replace("'", "").replace("[", "").replace("]", "")

    # * if existing con was given, dont close
    if not con:
        con_.close()

    return out


def print_meta(path_sqlite: str | Path) -> None:
    """
    Prints metadata information from a given SQLite database file.

    Args:
        path_sqlite (str | Path): The path to the SQLite database file.

    Returns:
        None
    """
    # * resolve possible ~ in path
    path = Path(expanduser(path_sqlite))

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path_sqlite}")

    if path.suffix == ".db" or path.suffix == ".sqlite":
        con = sqlite3.connect(path)
        meta = pd.read_sql_query("SELECT * from _meta", con)
    elif path.suffix == ".duckdb":
        con = ddb.connect(path.as_posix(), read_only=True)
        meta = con.sql("SELECT * from _meta").to_df()
    else:
        raise ValueError(f"Unsupported file type: {path}")

    deli = meta.get("data_delivered_at")
    trans = meta.get("table_transmitted_at")
    creat = meta["table_created_at"]
    tag = meta.get("tag")

    print(f"{'sqlite db file:': <25}{path.name}")
    if tag is not None:
        print(f"{'data tag:': <25}{tag[0]}")
    if deli is not None:
        print(f"{'last kkr data import:': <25}{deli[0][:19]}")
    if creat is not None:
        print(f"{'sql table created:': <25}{creat[0][:19]}")
    if trans is not None:
        print(f"{'sql table transmitted:': <25}{trans[0][:19]}")
    print(
        f"{'document created:': <25}{dt.datetime.now().isoformat(sep=' ', timespec='seconds')}"
    )

    con.close()
    return


def load_from_mssql(
    query: str,
    host: str,  # This will be either the host name or the environment variable name
    db: str,  # This will be either the db name or the environment variable name
    use_env: bool = True,
) -> pd.DataFrame:
    """
    Loads data from an MSSQL database into a Pandas DataFrame.

    Args:
        query (str): The SQL query to execute for loading the data.
        host (str): The SQL server host or the environment variable name for it.
        db (str): The database name or the environment variable name for it.
        use_env (bool): Whether to use environment variables for the connection. Default is True.

    Returns:
        pd.DataFrame: The loaded data in a DataFrame.
    """
    if use_env:
        # Load environment variables
        load_dotenv(find_dotenv())
        host = os.getenv(host)
        db = os.getenv(db)

    # Ensure host and db are provided
    if not host or not db:
        raise ValueError(
            "❌ Both host and db must be provided either through environment variables or directly as arguments."
        )

    # Establish SQL connection
    con = connect_sql(host=host, db=db, dbms="mssql", use_env=False)

    # Load the DataFrame from SQL
    df = pd.read_sql(query, con)
    print(f"✔️ Data loaded from [{db}]")

    return df


def save_to_mssql(
    df: pd.DataFrame,
    con: object = None,
    host: str = "",  # This will be either the host name or the environment variable name
    db: str = "",  # This will be either the db name or the environment variable name
    table_name: str = "Table",
    schema_name: str = "dbo",
    use_env: bool = True,
    add_id: bool = False,
    add_timestamp: bool = False,
    ask_user: bool = True,
) -> None:
    """
    Saves data to an MSSQL database. Uses either a connection object or environment variables / host and db for the connection.

    Args:
        df: The input DataFrame.
        con (object, optional): The connection object. Defaults to None.
        host (str, optional): The SQL server host or the environment variable name for it.
        db (str, optional): The database name or the environment variable name for it.
        use_env (bool): Whether to use environment variables for the connection. Default is True.
        table_name (str): Name of the table to save data to.
        schema_name (str): Name of the schema. Default is 'dbo'.
        add_id (bool): Whether to add an id column. Default is False.
        add_timestamp (bool): Whether to add a craeted_at column. Default is False.
        ask_user (bool): Whether to ask the user for confirmation. Default is True.
    """
    df_ = df.copy()

    if ask_user:
        user_input = input(
            f"🚨 Do you want to write the data to the MSSQL table [{schema_name}].[{table_name}] on [{host}].[{db}]? ([y]/n): "
        )
        if user_input.lower() != "y":
            print("❌ Aborted.")
            return

    # Establish SQL connection
    if con is None:
        if use_env:
            # Load environment variables
            load_dotenv(find_dotenv())
            host = os.getenv(host)
            db = os.getenv(db)
        # Ensure host and db are provided
        if not host or not db:
            raise ValueError(
                "❌ Both host and db must be provided either through environment variables or directly as arguments."
            )
        con = connect_sql(host=host, db=db, dbms="mssql", use_env=False)

    # add timestamp column to df
    if add_timestamp and "created_at" not in df_.columns:
        df_["created_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # add id column to df
    if add_id and "id" not in df_.columns:
        df_.insert(0, "id", range(1, len(df_) + 1))

    # Save the DataFrame to SQL
    print("⏳ writing data to MSSQL...")
    df_.to_sql(
        table_name, schema=schema_name, con=con, if_exists="replace", index=False
    )
    print(f"✅ data written to [{schema_name}].[{table_name}] on [{host}].[{db}]")
    return


def load_file_to_duckdb(
    con: ddb.DuckDBPyConnection, path: str | Path, **kwargs
) -> ddb.DuckDBPyRelation:
    """
    Loads data from various sources into a duckdb database, using these pandas read functions:
        - read_csv
        - read_parquet
        - read_excel
    It also handles url paths.

    Args:
        con (duckdb connection): The DuckDB connection to use.
        path (str): The path to the resource to load.
        kwargs: Additional keyword arguments to pass to the respective pandas read function.
        Examples:
            - sep=";"
            - encoding="utf-8-sig"
            - sheet_name=0

    Returns:
        duckdb.DuckDBPyRelation
    """
    if not path:
        raise FileNotFoundError(f"❌ path is empty")

    # * is file path
    if not is_url(str(path)):
        if not Path(path).exists():
            raise FileNotFoundError(f"❌ file not found: {path}")

        # ! only str paths from here on
        path = Path(path).as_posix()

        # * resolve possible home in path
        if "~" in path:
            path = Path(expanduser(path))

    if path.endswith((".csv", ".txt")):
        # * ; is default separator
        kwargs.setdefault("sep", ";")
        df = pd.read_csv(path, **kwargs)
    elif path.endswith(".parquet"):
        df = pd.read_parquet(path, **kwargs)
    elif path.endswith(".xlsx"):
        df = pd.read_excel(path, **kwargs)

    db = con.from_df(df)
    return db


def sqlite_to_duckdb(sqlite_path: str | Path, debug: bool = False) -> None:
    """
    Converts a SQLite database to a DuckDB database.

    Args:
        sqlite_path (str | Path): The path to the SQLite file.
        debug (bool, optional): If True, limits the export to 1000 rows per table for debugging. Defaults to False.

    Returns:
        None
    """
    sqlite_path = Path(sqlite_path)
    if not sqlite_path.exists() or sqlite_path.suffix not in [".sqlite", ".db"]:
        raise ValueError("Please provide a valid .sqlite file path / name (.sqlite or .db)")

    duckdb_path = sqlite_path.with_suffix(".duckdb")

    # Open connections
    sqlite_conn = sqlite3.connect(sqlite_path)
    duckdb_conn = ddb.connect(duckdb_path.as_posix())

    try:
        # Get table list
        cursor = sqlite_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]

        for table in tables:
            print(f"Exporting {table}...")
            query = f"SELECT * FROM {table} LIMIT 1000" if debug else f"SELECT * FROM {table}"
            df = pd.read_sql_query(query, sqlite_conn)
            duckdb_conn.register("df_view", df)
            duckdb_conn.execute(f"CREATE TABLE {table} AS SELECT * FROM df_view")
            duckdb_conn.unregister("df_view")

        print(f"✅ Export {'(debug mode)' if debug else ''} complete. DuckDB file: {duckdb_path}")

    finally:
        sqlite_conn.close()
        duckdb_conn.close()



def mssql_to_duckdb(
    mssql_conn: sqlalchemy.engine.Connection,
    tables: List[str],
    schema: Optional[str] = None,
    duckdb_path: Union[str, Path] = "output.duckdb",
    debug: bool = False,
    chunksize: Optional[int] = 100_000,
) -> None:
    """
    Converts selected tables from a MSSQL database to a DuckDB database with optional chunking.

    Args:
        mssql_conn (sqlalchemy.engine.Connection): An active SQLAlchemy MSSQL connection object.
        tables (List[str]): List of table names to export.
        schema (str | None, optional): If set, prepends schema to table names. If None or "", assumes schema is in the table name.
        duckdb_path (str | Path, optional): Output DuckDB file path. Defaults to "output.duckdb".
        debug (bool, optional): If True, only exports top 1000 rows. Defaults to False.
        chunksize (int | None, optional): Number of rows per chunk. If None, loads entire table at once. Defaults to 100,000.

    Returns:
        None
    """
    duckdb_path = Path(duckdb_path)
    print(f"Exporting from MSSQL to DuckDB: {duckdb_path}")

    # * as_posix to avoid issues on windows
    duckdb_conn = ddb.connect(duckdb_path.as_posix())

    try:
        for table in tables:
            full_table_name = f"{schema}.{table}" if schema else table
            print(f"Exporting {full_table_name}...")

            if debug:
                query = f"SELECT TOP 1000 * FROM {full_table_name}"
                df = pd.read_sql_query(query, mssql_conn)
                table_name_only = table.split(".")[-1].strip("[]")
                duckdb_conn.register("df_view", df)
                duckdb_conn.execute(
                    f"CREATE TABLE {table_name_only} AS SELECT * FROM df_view"
                )
                duckdb_conn.unregister("df_view")
            else:
                query = f"SELECT * FROM {full_table_name}"
                table_name_only = table.split(".")[-1].strip("[]")
                first_chunk = True

                for chunk in pd.read_sql_query(query, mssql_conn, chunksize=chunksize):
                    duckdb_conn.register("df_view", chunk)

                    if first_chunk:
                        duckdb_conn.execute(
                            f"CREATE TABLE {table_name_only} AS SELECT * FROM df_view"
                        )
                        first_chunk = False
                    else:
                        duckdb_conn.execute(
                            f"INSERT INTO {table_name_only} SELECT * FROM df_view"
                        )

                    duckdb_conn.unregister("df_view")

        print(
            f"✅ Export {'(debug mode)' if debug else ''} complete. DuckDB file: {duckdb_path}"
        )

    finally:
        duckdb_conn.close()
        

