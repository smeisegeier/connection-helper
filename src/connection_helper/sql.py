import os
from os.path import expanduser


import sqlite3
from urllib.parse import urlparse
import pandas as pd
import duckdb as ddb
from pathlib import Path
import datetime as dt

import sqlalchemy
from sqlalchemy import create_engine, text
import sqlalchemy.engine
from sqlalchemy_utils import create_database, database_exists

from typing import List, Optional, Union, Literal
from dotenv import load_dotenv, find_dotenv

import subprocess
from datetime import datetime, timedelta # Import timedelta as well



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


def print_meta(path: str | Path) -> None:
    """
    Prints metadata information from a given SQLite or DuckDB database file.

    Args:
        path_sqlite (str | Path): The path to the database file.

    Returns:
        None
    """
    # * resolve possible ~ in path
    path = Path(expanduser(path))

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    con = None
    try:
        if path.suffix in {".db", ".sqlite"}:
            con = sqlite3.connect(path)
            try:
                meta = pd.read_sql_query("SELECT * FROM _meta", con)
            except Exception:
                con.close()
                raise
        elif path.suffix == ".duckdb":
            con = ddb.connect(path.as_posix(), read_only=True)
            try:
                meta = con.sql("SELECT * FROM _meta").to_df()
            except Exception:
                con.close()
                raise
        else:
            raise ValueError(f"Unsupported file type: {path}")

        deli = meta.get("data_delivered_at")
        trans = meta.get("table_transmitted_at")
        creat = meta.get("table_created_at")
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

    finally:
        if con is not None:
            con.close()

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


def load_sqlite_to_duckdb(sqlite_path: str | Path, debug: bool = False) -> None:
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
        return



def load_mssql_to_duckdb( # Renamed function
        con_source,
        file_db: str,
        list_tables: list[str],
        dict_meta: dict = None,
        delete_csv_after: bool = True,
        top_n_rows: int = 0,
        verbose: bool = True, # Retain verbose for general function progress messages
) -> None:
    """
    Load SQL tables from a source MSSQL database into a DuckDB database using bcp for export.
    This function leverages bcp for efficient data extraction and DuckDB's COPY FROM for fast ingestion,
    explicitly defining schema to handle bcp's lack of header output.
    It ensures NULLs in character columns are converted to empty strings, and now also
    removes embedded Carriage Return (CHAR(13)), Line Feed (CHAR(10)), AND TAB (CHAR(9)) characters
    from character/text columns directly in the SQL Server SELECT query
    to prevent CSV parsing issues.
    It includes a post-processing step to remove literal '\0' bytes from the CSV files.
    BCP's raw stdout/stderr output is now suppressed.
    This function will infer DuckDB column types from MSSQL schema or default to VARCHAR,
    and does NOT accept type overrides to ensure all rows are loaded initially.

    Args:
        con_source (object): The connection object to the source MSSQL database (e.g., SQLAlchemy engine or Connection).
                             Used to extract the SQL Server host and database name for bcp.
        file_db (str): The path to the DuckDB database file (e.g., 'my_database.duckdb').
        list_tables (list[str]): A list of SQL tables to load. Each table can be specified as a string
                                 or a list of two strings, where the first string is the table name
                                 in the source database and the second string is the table name in
                                 the DuckDB database (optional).
        dict_meta (dict, optional): A dictionary containing metadata to be written to the DuckDB database.
                                    The keys of this dictionary will become column names in a single-row
                                    '_meta' table, and their values will be inserted.
                                    Example: {'table_created_at': '2023-10-26 10:00:00', 'source_version': '1.0'}.
                                    Defaults to None.
        delete_csv_after (bool, optional): If True, the intermediate CSV files created by bcp will be
                                           deleted after successful loading into DuckDB. Defaults to True.
        top_n_rows (int, optional): The number of rows to load from each table. Defaults to 0 (all rows).
                                    This limit is applied directly in the bcp query.
        verbose (bool, optional): Whether to print general progress messages. Defaults to True.

    Returns:
        None

    Description:
        This function performs the following steps:
        1. Checks if the target DuckDB file already exists.
        2. Establishes a connection to the DuckDB database.
        3. Writes metadata to the DuckDB database if dict_meta is provided, creating a single-row table.
        4. For each table:
           a. Extracts column names and types from the source SQL Server database in the correct order.
           b. Creates the table in DuckDB with the correct schema (inferred from MSSQL).
           c. Constructs a bcp command to export data from MSSQL to a temporary CSV file
              in a '.local' directory, using a trusted connection. It now includes logic to
              cleanse embedded newlines AND tabs and ensure NULLs are exported as empty strings for character columns.
           d. Executes the bcp command using subprocess (output suppressed).
           e. **Post-processes the CSV file to remove any literal '\0' bytes.**
           f. Copies the data from the cleaned CSV into the pre-defined DuckDB table (without header inference).
           g. Optionally deletes the CSV file.
        5. Closes the DuckDB connection.
    """
    # Capture the start time when the function begins execution
    function_start_time = datetime.now()

    # Helper to get relative timestamp for verbose output
    def get_relative_timestamp():
        elapsed_time = datetime.now() - function_start_time
        total_seconds = int(elapsed_time.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"[{hours:02d}:{minutes:02d}:{seconds:02d}]"

    # * Check if db already exists
    if os.path.exists(file_db):
        print(f"{get_relative_timestamp()} ❌ {file_db} already exists. Exiting.")
        return

    # * Establish connection to DuckDB
    con_duckdb = ddb.connect(database=file_db)

    # * Create .local directory for temporary CSVs if it doesn't exist
    local_dir = ".local"
    os.makedirs(local_dir, exist_ok=True)
    if verbose:
        print(f"{get_relative_timestamp()} Ensuring temporary CSV directory exists: {local_dir}")

    # * Write meta table if dict was given
    if dict_meta is not None:
        if verbose:
            print(f"{get_relative_timestamp()} Creating and populating metadata table '_meta'...")
        try:
            # Dynamically create columns based on dict_meta keys, all as VARCHAR
            meta_column_names = list(dict_meta.keys())
            column_definitions = [f'"{col_name}" VARCHAR' for col_name in meta_column_names]
            create_meta_sql = f"CREATE TABLE _meta ({', '.join(column_definitions)});"
            
            con_duckdb.execute("DROP TABLE IF EXISTS _meta;") # Drop existing meta if any (shouldn't be in new DB)
            con_duckdb.execute(create_meta_sql)

            # Prepare values for insertion
            placeholders = ', '.join(['?' for _ in meta_column_names])
            insert_meta_sql = f"INSERT INTO _meta ({', '.join([f'"{c}"' for c in meta_column_names])}) VALUES ({placeholders});"
            
            # Convert all metadata values to string for VARCHAR columns
            meta_values = [str(v) for v in dict_meta.values()]
            con_duckdb.execute(insert_meta_sql, meta_values)

            if verbose:
                print(f"{get_relative_timestamp()} Metadata table '_meta' created and populated.")
        except ddb.Error as e:
            print(f"{get_relative_timestamp()} ❌ Error creating or populating _meta table: {e}")

    # Extract server and database name from SQLAlchemy connection URL
    server_name = None
    database_name = None
    try:
        if hasattr(con_source, 'engine') and hasattr(con_source.engine, 'url'):
            server_name = con_source.engine.url.host
            database_name = con_source.engine.url.database
        elif hasattr(con_source, 'url'): # For SQLAlchemy Engine objects
            server_name = con_source.url.host
            database_name = con_source.url.database
        else:
            raise ValueError("Could not extract server/database name from con_source. Please ensure it's an SQLAlchemy Engine or Connection object.")

        if not server_name or not database_name:
            raise ValueError("Extracted server name or database name is empty. Please ensure con_source is correctly configured.")
    except AttributeError:
        print(f"{get_relative_timestamp()} Error: con_source does not appear to be a valid SQLAlchemy Engine or Connection object.")
        print(f"{get_relative_timestamp()} Please ensure con_source is an SQLAlchemy Engine or Connection object and provides host and database.")
        return
    except ValueError as e:
        print(f"{get_relative_timestamp()} Error extracting server/database name: {e}")
        return

    is_list_nested = all(isinstance(i, list) for i in list_tables)

    # Basic type mapping from common SQL Server types to DuckDB types
    # This map might need to be extended based on the exact types in your MSSQL database
    sql_to_duckdb_type_map = {
        'bit': 'BOOLEAN',
        'tinyint': 'TINYINT',
        'smallint': 'SMALLINT',
        'int': 'INTEGER',
        'bigint': 'BIGINT',
        'real': 'REAL', # 4-byte floating point
        'float': 'DOUBLE', # 8-byte floating point
        'decimal': 'DECIMAL',
        'numeric': 'DECIMAL',
        'money': 'DECIMAL(19,4)', # Common precision for money
        'smallmoney': 'DECIMAL(10,4)',
        'char': 'VARCHAR',
        'varchar': 'VARCHAR',
        'nvarchar': 'VARCHAR',
        'text': 'VARCHAR',
        'ntext': 'VARCHAR',
        'date': 'DATE',
        'datetime': 'TIMESTAMP',
        'datetime2': 'TIMESTAMP',
        'smalldatetime': 'TIMESTAMP',
        'time': 'TIME',
        'uniqueidentifier': 'UUID',
        'binary': 'BLOB',
        'varbinary': 'BLOB',
        'image': 'BLOB',
        # Add more as needed, e.g., xml, geography, geometry
    }

    for item in list_tables:
        if is_list_nested:
            table_sql_full = item[0]
            table_friendly = (
                item[1]
                if item[1]
                else item[0] if "." not in item[0] else item[0].split(".")[1]
            )
        else:
            table_sql_full = item
            table_friendly = item if "." not in item else item.split(".")[1]

        # Extract schema and table name from table_sql_full for INFORMATION_SCHEMA query
        sql_schema = 'dbo' # Default to 'dbo' if no schema specified
        sql_table_name = table_sql_full.strip('[]')
        if "." in table_sql_full:
            parts = table_sql_full.split('.')
            sql_schema = parts[0].strip('[]')
            sql_table_name = parts[1].strip('[]')


        csv_filename = f"{table_friendly}.csv"
        csv_path = os.path.join(local_dir, csv_filename)

        if verbose:
            print(f"{get_relative_timestamp()} Processing: {table_sql_full} -> {table_friendly}")
            print(f"{get_relative_timestamp()}   Temporary CSV will be: {csv_path}")

        # --- Get column names and types from source database for DuckDB schema ---
        column_definitions = []
        source_columns_with_types = [] # Store (column_name, sql_type) for bcp query construction
        try:
            # Step 1: Get actual column names in their exact order from MSSQL
            # This ensures the order matches what bcp will output for SELECT *
            test_query_for_order = f"SELECT TOP 1 * FROM {table_sql_full};"
            if verbose:
                print(f"{get_relative_timestamp()}   Getting column order from MSSQL using: {test_query_for_order}")

            proxy_for_cols = con_source.execute(text(test_query_for_order))
            actual_col_names_ordered = list(proxy_for_cols.keys())
            proxy_for_cols.close() # Explicitly close the result proxy to free the connection

            # Step 2: Query INFORMATION_SCHEMA for types based on these ordered names
            for col_name in actual_col_names_ordered:
                sql_type = None # Initialize sql_type
                duckdb_type = None # Initialize duckdb_type

                # Original type inference logic remains the only path
                type_query = f"""
                    SELECT DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{sql_schema}' AND TABLE_NAME = '{sql_table_name}' AND COLUMN_NAME = '{col_name}';
                """
                sql_type_result = con_source.execute(text(type_query)).scalar_one_or_none()
                if sql_type_result:
                    sql_type = sql_type_result.lower()
                    duckdb_type = sql_to_duckdb_type_map.get(sql_type, 'VARCHAR') # Default to VARCHAR if not mapped
                else:
                    print(f"{get_relative_timestamp()}   Warning: Could not find type for column '{col_name}'. Defaulting to VARCHAR.")
                    sql_type = 'varchar' # Default type for bcp ISNULL logic
                    duckdb_type = 'VARCHAR'

                # Enclose column names in double quotes for DuckDB CREATE TABLE statement
                column_definitions.append(f'"{col_name}" {duckdb_type}')
                source_columns_with_types.append((col_name, sql_type)) # Store for bcp query construction

            if not column_definitions:
                print(f"{get_relative_timestamp()}   Warning: No column definitions could be determined for {table_sql_full}. Skipping table processing.")
                continue # Skip to next table if no columns

            # Create table in DuckDB with explicitly defined columns and types
            create_table_sql = f"CREATE TABLE {table_friendly} ({', '.join(column_definitions)});"
            con_duckdb.execute(f"DROP TABLE IF EXISTS {table_friendly};") # Drop before creating
            con_duckdb.execute(create_table_sql)
            if verbose:
                print(f"{get_relative_timestamp()}   Created DuckDB table schema: {table_friendly} ({', '.join(column_definitions)})")

        except Exception as e:
            print(f"{get_relative_timestamp()} ❌ Error getting schema for {table_sql_full}: {e}")
            continue # Skip to next table if schema cannot be retrieved

        # --- Construct the bcp query with ISNULL for character columns ---
        select_columns_for_bcp = []
        for col_name, sql_type in source_columns_with_types:
            # Apply ISNULL for character types to convert NULLs to empty strings
            # Add REPLACE for CHAR(13), CHAR(10), AND CHAR(9) for character/text types
            # Use MSSQL's bracket quoting for column names in the SELECT statement
            if sql_type in ['char', 'varchar', 'nvarchar', 'text', 'ntext']:
                # Nested REPLACE to remove CR, LF, AND TAB
                clean_string_sql = f"REPLACE(REPLACE(REPLACE(ISNULL([{col_name}], ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')"
                select_columns_for_bcp.append(clean_string_sql)
            else:
                select_columns_for_bcp.append(f"[{col_name}]") # Select directly

        select_clause_for_bcp = ", ".join(select_columns_for_bcp)
        if not select_clause_for_bcp: # Fallback if no columns or issue
            select_clause_for_bcp = "*" # Should not happen if column_definitions is not empty

        top_clause = f" TOP {top_n_rows}" if top_n_rows > 0 else ""
        # Use full table name for bcp, as it's an external command and needs context
        bcp_query = f"SELECT{top_clause} {select_clause_for_bcp} FROM {table_sql_full}"

        # Construct the bcp command for trusted connection
        bcp_command = [
            "bcp",
            bcp_query,
            "queryout",
            csv_path,
            "-w", # Wide character (UTF-16) export
            "-t\t", # Use tab as field terminator
            "-S", server_name,
            "-d", database_name,
            "-T" # Trusted connection
        ]

        try:
            if verbose:
                print(f"{get_relative_timestamp()}   Executing bcp command for {table_sql_full} (using -w for Unicode and cleansing newlines/tabs)...")

            # Execute bcp command (output is captured as bytes)
            result = subprocess.run(bcp_command, check=True, capture_output=True)
            
            # Decode stdout and stderr manually with error replacement (using latin-1 for diagnostic messages)
            bcp_stdout = result.stdout.decode('latin-1', errors='replace')
            bcp_stderr = result.stderr.decode('latin-1', errors='replace')

            # Only print bcp's "0 rows copied" message if verbose is True
            if verbose and "0 rows copied" in bcp_stdout:
                print(f"{get_relative_timestamp()}   bcp reported 0 rows copied for {table_sql_full}.")
            
            # If there's any stderr, print it (after decoding)
            if bcp_stderr:
                print(f"{get_relative_timestamp()}   BCP Stderr: {bcp_stderr.strip()}")

            # Check if the CSV file was actually created and is not empty
            if not os.path.exists(csv_path) or os.stat(csv_path).st_size == 0:
                if "0 rows copied" not in bcp_stdout: # Avoid duplicate error if 0 rows was already handled
                    raise FileNotFoundError(f"bcp did not create or populate the CSV file: {csv_path}. Check bcp output for errors.")

            # --- Post-process CSV to remove '\0' bytes ---
            if verbose:
                print(f"{get_relative_timestamp()}   Post-processing CSV: Removing '\\0' bytes from {csv_path} in streaming mode...")
            
            # Create a temporary output file to write cleaned content
            temp_csv_path = csv_path + ".tmp"
            with open(csv_path, 'r', encoding='utf-16', errors='replace') as f_in, \
                 open(temp_csv_path, 'w', encoding='utf-16') as f_out:
                for line in f_in:
                    f_out.write(line.replace('\0', '')) # Replace null characters line by line
            
            # Replace original file with cleaned temporary file
            os.replace(temp_csv_path, csv_path)

            if verbose:
                print(f"{get_relative_timestamp()}   Finished post-processing {csv_path}.")

            # Copy data into the pre-created table
            # HEADER FALSE because bcp queryout -w does NOT include headers.
            # DuckDB will use the schema defined in the CREATE TABLE statement.
            # Use 'utf-16' (lowercase) for DuckDB ENCODING
            if verbose:
                print(f"{get_relative_timestamp()}   Copying data from '{csv_path}' into {table_friendly} (FORMAT CSV, DELIMITER '\t', HEADER FALSE, NULL_PADDING TRUE, QUOTE '', STRICT_MODE FALSE, IGNORE_ERRORS TRUE, ENCODING 'utf-16').")
            con_duckdb.execute(f"COPY {table_friendly} FROM '{csv_path}' (FORMAT CSV, DELIMITER '\t', HEADER FALSE, NULL_PADDING TRUE, QUOTE '', STRICT_MODE FALSE, IGNORE_ERRORS TRUE, ENCODING 'utf-16');")

            if verbose:
                row_count = con_duckdb.execute(f"SELECT COUNT(*) FROM {table_friendly};").fetchone()[0]
                print(f"{get_relative_timestamp()} Finished loading {table_friendly}. Total rows: {row_count}")

            if delete_csv_after:
                os.remove(csv_path)
                if verbose:
                    print(f"{get_relative_timestamp()}   Deleted temporary CSV: {csv_path}")

        except subprocess.CalledProcessError as e:
            # Always print bcp errors if the command itself failed
            print(f"{get_relative_timestamp()} ❌ Error executing bcp for table {table_sql_full}:")
            print(f"{get_relative_timestamp()}   Command: {' '.join(e.cmd)}")
            # Decode stdout and stderr from the exception object as well
            error_stdout = e.stdout.decode('latin-1', errors='replace') if e.stdout else ""
            error_stderr = e.stderr.decode('latin-1', errors='replace') if e.stderr else ""
            print(f"{get_relative_timestamp()}   Return Code: {e.returncode}")
            print(f"{get_relative_timestamp()}   Stdout: {error_stdout.strip()}")
            print(f"{get_relative_timestamp()}   Stderr: {error_stderr.strip()}")
            print(f"{get_relative_timestamp()}   Please ensure 'bcp' is in your system's PATH, you have necessary database permissions, and the database name is correct.")
        except Exception as e:
            # Catch-all for other exceptions during DuckDB load
            print(f"{get_relative_timestamp()} ❌ Error processing table {table_sql_full} during DuckDB load.")
            print(f"{get_relative_timestamp()}   Exception Type: {type(e)}")
            print(f"{get_relative_timestamp()}   Exception Message: {e}") # This is str(e)
            print(f"{get_relative_timestamp()}   Full Exception Object (repr): {repr(e)}") # This will give more detail
            print(f"{get_relative_timestamp()}   Suggestion: Inspect the temporary CSV file at: {csv_path}")
            print(f"{get_relative_timestamp()}   This error indicates a problem during the DuckDB COPY operation. "
                  f"Check the CSV file's content and ensure the DuckDB schema matches the CSV data types.")
            continue # Skip to next table if processing current table fails

    # * Close the DuckDB connection
    con_duckdb.close()
    if verbose:
        print(f"{get_relative_timestamp()} Successfully created DuckDB file: {file_db}")

    return


def apply_duckdb_type_overrides(
    file_db: str,
    duckdb_column_type_overrides: dict, # Renamed argument
    verbose: bool = True,
) -> None:
    """
    Applies column type overrides to tables in an existing DuckDB database.
    This function iterates through specified tables and columns, attempting to alter
    their data types. It uses TRY_CAST to perform conversions, ensuring that if a
    value cannot be converted to the new type, it becomes NULL, preventing row loss.

    Args:
        file_db (str): The path to the existing DuckDB database file.
        duckdb_column_type_overrides (dict): A dictionary where keys are table names (strings),
                                            and values are dictionaries mapping column names (strings)
                                            to their desired DuckDB data types (strings).
                                            Example: {'MyTable': {'DateCol': 'DATE', 'IDCol': 'BIGINT'}}
        verbose (bool, optional): Whether to print progress messages. Defaults to True.

    Returns:
        None
    """
    con_duckdb = None
    try:
        # Check if db exists before attempting to connect
        if not os.path.exists(file_db):
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Error: DuckDB file '{file_db}' not found. Exiting.")
            return

        con_duckdb = ddb.connect(database=file_db)
        
        if verbose:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Applying type overrides to tables in '{file_db}'...")

        # Updated loop to use the new argument name
        for table_name, column_overrides in duckdb_column_type_overrides.items():
            if verbose:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing table: '{table_name}'")
            
            # Check if table exists
            table_info = con_duckdb.execute(f"PRAGMA table_info('{table_name}');").fetchall()
            if not table_info:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Warning: Table '{table_name}' not found in '{file_db}'. Skipping its column overrides.")
                continue

            for col_name, target_type in column_overrides.items():
                if verbose:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}]   Attempting to alter column '{col_name}' in '{table_name}' to '{target_type}'...")
                
                # Get current column type to check if it's already the target type or needs conversion
                # Using PRAGMA table_info to get column details
                current_col_details = con_duckdb.execute(f"PRAGMA table_info('{table_name}');").fetchall()
                current_type_found = None
                for col_detail in current_col_details:
                    if col_detail[1] == col_name: # col_detail[1] is the column name
                        current_type_found = col_detail[2] # col_detail[2] is the column type
                        break

                if current_type_found is None:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}]   Warning: Column '{col_name}' not found in table '{table_name}'. Skipping override for this column.")
                    continue

                # If current type is already the target type (case-insensitive check)
                if current_type_found.lower() == target_type.lower():
                    if verbose:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}]   Column '{col_name}' in '{table_name}' is already '{target_type}'. Skipping.")
                    continue

                try:
                    # DuckDB's ALTER TABLE syntax for type conversion
                    # Using TRY_CAST to convert, setting invalid values to NULL
                    # Double quotes for table and column names to handle special characters or keywords
                    alter_sql = f"ALTER TABLE \"{table_name}\" ALTER COLUMN \"{col_name}\" SET DATA TYPE {target_type} USING TRY_CAST(\"{col_name}\" AS {target_type});"
                    con_duckdb.execute(alter_sql)
                    if verbose:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}]   Successfully altered column '{col_name}' to '{target_type}' in '{table_name}'.")
                except ddb.Error as e:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Error altering column '{col_name}' in '{table_name}' to '{target_type}': {e}")
                    print(f"[{datetime.now().strftime('%H:%M:%S')}]   This column might contain data incompatible with '{target_type}'. "
                          f"The values that could not be cast have been set to NULL. Consider manual inspection or casting to VARCHAR if data quality is uncertain.")
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ An unexpected error occurred during type override application: {e}")
    finally:
        if con_duckdb:
            con_duckdb.close()
            if verbose:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] DuckDB connection closed.")
