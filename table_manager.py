# table_manager.py
import argparse
import psycopg2
import configparser
import os
import sys
from datetime import datetime

# --- Configuration ---
DEFAULT_CONFIG_FILE = 'config.conf'
DEFAULT_SECRETS_FILE = 'secrets.conf' # Для согласованности, хотя не используется напрямую

def load_config(config_file=DEFAULT_CONFIG_FILE, secrets_file=DEFAULT_SECRETS_FILE):
    """Loads configuration from main config and secrets files."""
    config = configparser.ConfigParser()
    config.optionxform = str # Сохранять регистр ключей

    # 1. Загрузить основной файл конфигурации
    if not os.path.exists(config_file):
        print(f"Error: Main configuration file '{config_file}' not found.")
        sys.exit(1)
    print(f"Loading main config from: {config_file}")
    config.read(config_file)

    # 2. Загрузить файл с секретами (если он существует)
    # table_manager.py не использует секреты напрямую, но может загрузить их
    # если они содержат дополнительные настройки (например, schema)
    if os.path.exists(secrets_file):
        print(f"Loading secrets (for potential DB schema) from: {secrets_file}")
        config.read(secrets_file)

    # Проверка обязательных секций
    required_sections = ['DATABASE', 'API', 'TABLES']
    # Добавляем секции схем таблиц
    for table_key in ['bonds', 'quotas', 'coupons', 'amortizations', 'offers']:
        required_sections.append(f"TABLE_SCHEMA:{table_key}")

    for section in required_sections:
        if not config.has_section(section):
             print(f"Error: Configuration section '[{section}]' not found.")
             sys.exit(1)

    return config

# --- Argument Parsing ---
def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Utility for managing MOEX bond data tables.")
    parser.add_argument(
        "--action",
        required=True,
        choices=['create', 'drop', 'clear', 'stats', 'list_tables'],
        help="Specify the action to perform on the table(s)."
    )
    parser.add_argument(
        "--table",
        choices=['bonds', 'quotas', 'coupons', 'amortizations', 'offers'],
        help="Specify the table to operate on. If omitted, action applies to all tables (if applicable)."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_FILE,
        help=f"Path to the main configuration file (default: {DEFAULT_CONFIG_FILE})."
    )
    parser.add_argument(
        "--secrets",
        type=str,
        default=DEFAULT_SECRETS_FILE,
        help=f"Path to the secrets configuration file (default: {DEFAULT_SECRETS_FILE})."
    )
    return parser.parse_args()

# --- Database Interaction ---
def get_db_connection(config):
    """Establishes a database connection."""
    try:
        conn = psycopg2.connect(
            host=config.get('DATABASE', 'host'),
            port=config.get('DATABASE', 'port'),
            database=config.get('DATABASE', 'database'),
            user=config.get('DATABASE', 'user'),
            password=config.get('DATABASE', 'password')
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def get_table_name(config, table_key):
    """Gets the actual DB table name from the config."""
    return config.get('TABLES', table_key, fallback=table_key)

def get_db_schema(config):
    """Gets the database schema from the config."""
    return config.get('DATABASE', 'schema', fallback='public')

def get_all_table_keys():
    """Returns the list of standard table keys."""
    return ['bonds', 'quotas', 'coupons', 'amortizations', 'offers']

def execute_query(conn, query, params=None, fetch=False):
    """Executes a query and optionally fetches results."""
    try:
        with conn.cursor() as cur:
            # print(f"Executing query: {query}") # Debug
            # if params: print(f"With params: {params}") # Debug
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
            conn.commit()
    except psycopg2.Error as e:
        print(f"Database error executing query: {e}")
        # print(f"Query: {query}") # Uncomment for debugging
        # if params:
        #     print(f"Params: {params}")
        conn.rollback()
        return None
    except Exception as e:
        print(f"Unexpected error executing query: {e}")
        # print(f"Query: {query}") # Uncomment for debugging
        # if params:
        #     print(f"Params: {params}")
        conn.rollback()
        return None
    return True

# --- Actions ---
def list_tables(conn, config):
    """Lists all configured tables and their DB names."""
    print("--- Configured Tables ---")
    schema = get_db_schema(config)
    for key in get_all_table_keys():
        db_name = get_table_name(config, key)
        exists_query = """
            SELECT EXISTS (
               SELECT FROM information_schema.tables
               WHERE  table_schema = %s
               AND    table_name   = %s
            );
        """
        result = execute_query(conn, exists_query, (schema, db_name), fetch=True)
        if result is not None and len(result) > 0 and len(result[0]) > 0:
            exists = result[0][0]
            status = "EXISTS" if exists else "NOT FOUND"
        else:
            status = "CHECK FAILED"
        print(f"  {key:<15} -> {db_name:<20} ({status})")


def create_table(conn, table_key, config):
    """Creates a specific table based on its key, reading schema from config."""
    table_name = get_table_name(config, table_key)
    db_schema = get_db_schema(config)
    schema_section = f"TABLE_SCHEMA:{table_key}"

    if not config.has_section(schema_section):
        print(f"Error: Configuration section '[{schema_section}]' not found for table '{table_key}'.")
        return False

    # Получить определения столбцов из конфига
    columns_def = []
    for column_name, column_def in config.items(schema_section):
        # column_def может содержать тип и DEFAULT
        columns_def.append(f"    {column_name} {column_def}")

    if not columns_def:
        print(f"Error: No columns defined in '[{schema_section}]'.")
        return False

    columns_sql = ",\n".join(columns_def)
    # Простой способ создания индекса на secid или isin, если они есть
    index_sql_parts = []
    if config.has_option(schema_section, 'secid'):
         index_sql_parts.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_secid ON {db_schema}.{table_name} (secid);")
    if config.has_option(schema_section, 'isin'):
         index_sql_parts.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_isin ON {db_schema}.{table_name} (isin);")
    if config.has_option(schema_section, 'tradedate'):
         index_sql_parts.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_tradedate ON {db_schema}.{table_name} (tradedate);")
    if config.has_option(schema_section, 'coupondate'):
         index_sql_parts.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_coupondate ON {db_schema}.{table_name} (coupondate);")
    if config.has_option(schema_section, 'amortdate'):
         index_sql_parts.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_amortdate ON {db_schema}.{table_name} (amortdate);")
    if config.has_option(schema_section, 'offerdate'):
         index_sql_parts.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_offerdate ON {db_schema}.{table_name} (offerdate);")

    index_sql = "\n".join(index_sql_parts)

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {db_schema}.{table_name} (
{columns_sql}
        );
    """
    full_sql = create_table_sql
    if index_sql:
        full_sql += "\n" + index_sql

    print(f"Creating table {db_schema}.{table_name}...")
    try:
        # Разделить SQL на отдельные операторы
        statements = [stmt.strip() for stmt in full_sql.split(';') if stmt.strip()]
        for stmt in statements:
            if stmt:
                if not execute_query(conn, stmt):
                    raise Exception(f"Failed to execute statement: {stmt[:50]}...")
        print(f"Table {db_schema}.{table_name} created successfully (or already existed).")
        return True
    except Exception as e:
        print(f"Failed to create table {db_schema}.{table_name}: {e}")
        return False


def drop_table(conn, table_key, config):
    """Drops a specific table."""
    table_name = get_table_name(config, table_key)
    db_schema = get_db_schema(config)
    query = f"DROP TABLE IF EXISTS {db_schema}.{table_name} CASCADE;"
    print(f"Dropping table {db_schema}.{table_name}...")
    result = execute_query(conn, query)
    if result:
        print(f"Table {db_schema}.{table_name} dropped successfully (if it existed).")
    else:
        print(f"Failed to drop table {db_schema}.{table_name}.")
    return result

def clear_table(conn, table_key, config):
    """Clears (TRUNCATE) a specific table."""
    table_name = get_table_name(config, table_key)
    db_schema = get_db_schema(config)
    query = f"TRUNCATE TABLE {db_schema}.{table_name};"
    print(f"Clearing table {db_schema}.{table_name}...")
    result = execute_query(conn, query)
    if result:
        print(f"Table {db_schema}.{table_name} cleared successfully.")
    else:
        print(f"Failed to clear table {db_schema}.{table_name}.")
    return result

def gather_statistics(conn, table_key, config):
    """Gathers statistics for a specific table."""
    table_name = get_table_name(config, table_key)
    db_schema = get_db_schema(config)

    print(f"--- Statistics for table: {db_schema}.{table_name} ---")

    # 1. Total row count
    count_query = f"SELECT COUNT(*) FROM {db_schema}.{table_name};"
    result = execute_query(conn, count_query, params=None, fetch=True)
    if result is not None and len(result) > 0:
        print(f"  Total Rows: {result[0][0]}")
    else:
        print("  Failed to get row count.")

    # 2. Row count by date (if applicable)
    date_columns = ['tradedate', 'coupondate', 'amortdate', 'offerdate', 'matdate']
    date_stats_found = False
    for date_col in date_columns:
        check_col_query = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = %s
                AND table_name = %s
                AND column_name = %s
            );
        """
        col_exists_result = execute_query(conn, check_col_query, (db_schema, table_name, date_col), fetch=True)
        if col_exists_result is not None and len(col_exists_result) > 0 and col_exists_result[0][0]:
            date_count_query = f"""
                SELECT {date_col}, COUNT(*) as cnt
                FROM {db_schema}.{table_name}
                WHERE {date_col} IS NOT NULL
                GROUP BY {date_col}
                ORDER BY {date_col} DESC
                LIMIT 10;
            """
            date_result = execute_query(conn, date_count_query, params=None, fetch=True)
            if date_result is not None:
                print(f"  Recent entries by {date_col}:")
                for row in date_result:
                    print(f"    {row[0]}: {row[1]} rows")
                date_stats_found = True
                break
            else:
                print(f"  Failed to get date statistics for {date_col}.")
    if not date_stats_found:
        print("  No standard date column found for recent entry statistics.")

    # 3. Distinct ISIN/SECID count (common key)
    key_columns_to_check = ['isin', 'secid']
    key_stats_found = False
    for key_col in key_columns_to_check:
        key_check_query = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = %s
                AND table_name = %s
                AND column_name = %s
            );
        """
        key_exists_result = execute_query(conn, key_check_query, (db_schema, table_name, key_col), fetch=True)
        if key_exists_result is not None and len(key_exists_result) > 0 and key_exists_result[0][0]:
            key_count_query = f"SELECT COUNT(DISTINCT {key_col}) FROM {db_schema}.{table_name};"
            key_result = execute_query(conn, key_count_query, params=None, fetch=True)
            if key_result is not None and len(key_result) > 0:
                 print(f"  Distinct {key_col.upper()}s: {key_result[0][0]}")
                 key_stats_found = True
                 break
            else:
                 print(f"  Failed to get distinct {key_col.upper()} count.")
    if not key_stats_found:
        print("  No standard key column ('isin', 'secid') found for distinct count.")

    # 4. Table size (approximate)
    size_query = "SELECT pg_size_pretty(pg_total_relation_size(%s));"
    full_table_name = f"{db_schema}.{table_name}"
    size_result = execute_query(conn, size_query, (full_table_name,), fetch=True)
    if size_result is not None and len(size_result) > 0 and len(size_result[0]) > 0:
        print(f"  Table Size (approx.): {size_result[0][0]}")
    else:
         print("  Failed to get table size.")

    print("--- End Statistics ---\n")
    return True

# --- Main Logic ---
def main():
    args = parse_arguments()
    config = load_config(args.config, args.secrets)

    conn = get_db_connection(config)
    if not conn:
        return

    try:
        if args.action == 'list_tables':
            list_tables(conn, config)
            return

        tables_to_process = [args.table] if args.table else get_all_table_keys()

        success = True
        for table_key in tables_to_process:
            if args.action == 'create':
                if not create_table(conn, table_key, config):
                    success = False
            elif args.action == 'drop':
                if not drop_table(conn, table_key, config):
                    success = False
            elif args.action == 'clear':
                if not clear_table(conn, table_key, config):
                    success = False
            elif args.action == 'stats':
                if not gather_statistics(conn, table_key, config):
                    pass # Continue processing others

        if not success:
            print(f"Some operations for action '{args.action}' failed.")
            sys.exit(1)
        else:
            processed_tables = ', '.join(tables_to_process) if args.table is None else args.table
            print(f"Action '{args.action}' completed successfully for table(s): {processed_tables}")

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()

