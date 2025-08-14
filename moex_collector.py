# moex_collector.py
import argparse
import requests
import json
import psycopg2
from datetime import datetime, timedelta
import configparser
import os
import sys


# --- Configuration ---
CONFIG_FILE = 'config.conf'
SECRETS_FILE = 'secrets.conf'

def load_config(config_file=CONFIG_FILE, secrets_file=SECRETS_FILE):
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
    if os.path.exists(secrets_file):
        print(f"Loading secrets from: {secrets_file}")
        config.read(secrets_file) # Загружаем секреты поверх основных настроек
    else:
        print(f"Warning: Secrets file '{secrets_file}' not found. Using defaults or environment variables if configured.")

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
    parser = argparse.ArgumentParser(description="Collect bond data from MOEX.")
    parser.add_argument(
        "--table",
        required=True,
        choices=['bonds', 'quotas', 'coupons', 'amortizations', 'offers'],
        help="Specify the data table to update."
    )
    parser.add_argument(
        "--date_from",
        type=str,
        help="Start date for data collection (YYYY-MM-DD). Defaults to yesterday."
    )
    parser.add_argument(
        "--date_to",
        type=str,
        help="End date for data collection (YYYY-MM-DD). Defaults to today."
    )
    parser.add_argument(
        "--isin",
        type=str,
        help="Comma-separated list of ISINs to process. If omitted, all bonds are processed."
    )
    parser.add_argument(
        "--mode",
        choices=['clear', 'update', 'overwrite'],
        default='update',
        help="Specify the mode of operation for the table: clear, update (default), or overwrite."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=CONFIG_FILE,
        help=f"Path to the main configuration file (default: {CONFIG_FILE})."
    )
    parser.add_argument(
        "--secrets",
        type=str,
        default=SECRETS_FILE,
        help=f"Path to the secrets configuration file (default: {SECRETS_FILE})."
    )
    return parser.parse_args()

# --- Date Handling ---
def get_date_range(args):
    """Determines the date range based on arguments or defaults."""
    if args.date_to:
        try:
            end_date = datetime.strptime(args.date_to, "%Y-%m-%d").date()
        except ValueError:
            print("Error: Invalid date format for --date_to. Use YYYY-MM-DD.")
            sys.exit(1)
    else:
        end_date = datetime.today().date()

    if args.date_from:
        try:
            start_date = datetime.strptime(args.date_from, "%Y-%m-%d").date()
        except ValueError:
            print("Error: Invalid date format for --date_from. Use YYYY-MM-DD.")
            sys.exit(1)
    else:
        start_date = end_date - timedelta(days=1) # Default to previous day

    if start_date > end_date:
        print("Error: Start date cannot be after end date.")
        sys.exit(1)

    return start_date, end_date

# --- MOEX API Interaction ---
def fetch_moex_data(url, params=None):
    """Fetches data from MOEX API."""
    try:
        # print(f"Fetching: {url} with params: {params}") # Debug print
        response = requests.get(url, params=params, timeout=30) # Add timeout
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        print(f"Error: Timeout fetching data from {url}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

def get_all_securities(config):
    """Fetches the list of all bond securities."""
    base_url = config.get('API', 'base_url')
    securities_url = f"{base_url}/engines/stock/markets/bonds/securities.json"
    securities = []
    start = 0
    limit = config.getint('API', 'default_limit', fallback=100)

    while True:
        params = {'start': start, 'limit': limit}
        data = fetch_moex_data(securities_url, params)
        if not data or 'securities' not in data or not data['securities'].get('data'):
            break

        securities.extend(data['securities']['data'])
        # Check pagination
        if 'securities.cursor' in data:
             cursor_data = data['securities.cursor']['data']
             if cursor_data and len(cursor_data) > 0:
                 index, total, pagesize = cursor_data[0]
                 if start + pagesize >= total:
                     break
                 start += pagesize
             else:
                 break
        else:
            break # No cursor info, assume done

    return securities

def get_isin_list(args, config):
    """Determines the list of ISINs to process."""
    if args.isin:
        return [isin.strip().upper() for isin in args.isin.split(',')]
    else:
        all_securities_data = get_all_securities(config)
        if not all_securities_data:
             print("Failed to fetch list of all securities.")
             return []
        isins = []
        columns = None
        if 'securities' in all_securities_data and 'columns' in all_securities_data['securities']:
             columns = all_securities_data['securities']['columns']
        elif 'columns' in all_securities_data:
             columns = all_securities_data['columns']

        if columns:
            try:
                isin_index = columns.index('ISIN')
            except ValueError:
                print("Error: Could not find 'ISIN' column in securities data structure.")
                return []

            for row in all_securities_data['securities']['data'] if 'securities' in all_securities_data else all_securities_data['data']:
                if len(row) > isin_index and row[isin_index]:
                    isins.append(row[isin_index])
        return isins

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

def get_db_schema(config):
    """Gets the database schema from the config."""
    return config.get('DATABASE', 'schema', fallback='public')

def clear_table(conn, table_name, config):
    """Clears all data from a specified table."""
    schema = get_db_schema(config)
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {schema}.{table_name};")
            conn.commit()
            print(f"Table {schema}.{table_name} cleared.")
    except psycopg2.Error as e:
        print(f"Error clearing table {schema}.{table_name}: {e}")
        conn.rollback()

def get_column_mapping(config, table_key):
    """Gets the mapping of API column names to DB column names from config."""
    schema_section = f"TABLE_SCHEMA:{table_key}"
    if not config.has_section(schema_section):
        print(f"Warning: Configuration section '[{schema_section}]' not found.")
        return {}

    # Предполагаем, что имена ключей в конфиге - это имена столбцов в БД
    # API использует те же имена, что и ключи в конфиге (в верхнем регистре)
    mapping = {}
    for db_column_name in config.options(schema_section):
        api_column_name = db_column_name.upper() # Предположение об именах API
        mapping[api_column_name] = db_column_name
    return mapping

def insert_data_generic(conn, table_name, data, config, table_key):
    """Generic function to insert data into a table, using config for column mapping."""
    if not data or 'data' not in data or not data['data']:
        # print(f"No data received for table {table_name}.")
        return

    schema = get_db_schema(config)
    api_columns = data.get('columns', []) # Имена столбцов из JSON API
    rows = data['data'] # Данные

    if not api_columns:
        print(f"No columns defined in API data for table {schema}.{table_name}.")
        return

    # Получить сопоставление имен столбцов API -> БД
    column_mapping = get_column_mapping(config, table_key)
    if not column_mapping:
        print(f"Failed to get column mapping for table {table_key}. Skipping insert.")
        return

    # Определить, какие столбцы API присутствуют в конфиге и их порядок
    db_columns_in_order = []
    api_indices_in_order = [] # Соответствующие индексы в списке api_columns
    for i, api_col_name in enumerate(api_columns):
        if api_col_name in column_mapping:
            db_col_name = column_mapping[api_col_name]
            db_columns_in_order.append(db_col_name)
            api_indices_in_order.append(i)
        # else:
        #     print(f"Warning: API column '{api_col_name}' not found in config for table '{table_key}'. Skipping.")

    if not db_columns_in_order:
        print(f"No matching columns found between API data and config for table {schema}.{table_name}.")
        return

    # Создать SQL-запрос на основе отфильтрованных и упорядоченных столбцов
    placeholders = ', '.join(['%s'] * len(db_columns_in_order))
    columns_str = ', '.join([f'"{col}"' for col in db_columns_in_order])

    insert_query = f"""
        INSERT INTO {schema}.{table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING;
    """

    inserted_count = 0
    try:
        with conn.cursor() as cur:
            for row in rows:
                # Извлечь только нужные значения из строки API в правильном порядке
                db_row = [row[i] if i < len(row) else None for i in api_indices_in_order]
                cur.execute(insert_query, db_row)
                inserted_count += cur.rowcount
            conn.commit()
            print(f"Attempted to insert {len(rows)} rows into {schema}.{table_name}. Rows affected: {inserted_count}.")
    except psycopg2.Error as e:
        print(f"Database error inserting data into {schema}.{table_name}: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Unexpected error during data insertion into {schema}.{table_name}: {e}")
        conn.rollback()


# --- Main Logic ---
def main():
    args = parse_arguments()
    config = load_config(args.config, args.secrets)

    start_date, end_date = get_date_range(args)
    isin_list = get_isin_list(args, config)

    if not isin_list and not args.isin:
        print("No ISINs found or specified. Exiting.")
        return

    print(f"Processing table: {args.table}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Mode: {args.mode}")
    print(f"ISINs to process: {isin_list if args.isin else 'All'}")

    conn = get_db_connection(config)
    if not conn:
        return

    base_url = config.get('API', 'base_url')
    default_limit = config.getint('API', 'default_limit', fallback=100)
    table_name = config.get('TABLES', args.table) # Получить имя таблицы БД

    try:
        if args.mode == 'clear':
            clear_table(conn, table_name, config)
            return

        if args.mode == 'overwrite':
            clear_table(conn, table_name, config)

        # --- Process based on table type ---
        if args.table == 'bonds':
            if args.isin:
                for isin in isin_list:
                    print(f"Fetching bond data for ISIN: {isin}")
                    url = f"{base_url}/engines/stock/markets/bonds/securities/{isin}.json"
                    data = fetch_moex_data(url)
                    if data and 'description' in data and data['description'].get('data'):
                         insert_data_generic(conn, table_name, data['description'], config, args.table)
                    else:
                         print(f"No data found for ISIN {isin} or unexpected structure.")
            else:
                 securities_data = get_all_securities(config)
                 if securities_data:
                     if 'securities' in securities_data and 'columns' in securities_data['securities'] and 'data' in securities_data['securities']:
                         insert_data_generic(conn, table_name, securities_data['securities'], config, args.table)
                     elif 'columns' in securities_data and 'data' in securities_data:
                          insert_data_generic(conn, table_name, securities_data, config, args.table)
                     else:
                         print("Unexpected data structure for all securities.")
                 else:
                     print("Failed to fetch data for all securities.")

        elif args.table == 'quotas':
            date = start_date
            while date <= end_date:
                print(f"Fetching quota data for date: {date}")
                if args.isin:
                    for isin in isin_list:
                        print(f"  -> ISIN: {isin}")
                        url = f"{base_url}/history/engines/stock/markets/bonds/securities/{isin}.json"
                        params = {'from': date.strftime("%Y-%m-%d"), 'till': date.strftime("%Y-%m-%d")}
                        data = fetch_moex_data(url, params)
                        if data and 'history' in data and data['history'].get('data'):
                            insert_data_generic(conn, table_name, data['history'], config, args.table)
                else:
                     url = f"{base_url}/history/engines/stock/markets/bonds/securities.json"
                     params = {'date': date.strftime("%Y-%m-%d"), 'start': 0, 'limit': default_limit}
                     has_more = True
                     while has_more:
                         print(f"  -> Fetching page starting at {params['start']}")
                         data = fetch_moex_data(url, params)
                         if data and 'history' in data and data['history'].get('data'):
                             insert_data_generic(conn, table_name, data['history'], config, args.table)
                         else:
                             print(f"    No data or unexpected structure for page {params['start']}.")

                         has_more = False
                         if data and 'history.cursor' in data:
                             cursor_data = data['history.cursor']['data']
                             if cursor_data and len(cursor_data) > 0:
                                 index, total, pagesize = cursor_data[0]
                                 if params['start'] + pagesize < total:
                                     params['start'] += pagesize
                                     has_more = True
                         if not data:
                             break

                date += timedelta(days=1)

        elif args.table in ['coupons', 'amortizations', 'offers']:
            data_block_map = {
                'coupons': 'coupons',
                'amortizations': 'amortizations',
                'offers': 'offers'
            }
            data_block = data_block_map[args.table]

            if args.isin:
                for isin in isin_list:
                    print(f"Fetching {args.table} data for ISIN: {isin}")
                    url = f"{base_url}/statistics/engines/stock/markets/bonds/bondization/{isin}.json"
                    params = {'limit': default_limit, 'start': 0}
                    has_more = True
                    while has_more:
                        print(f"  -> Fetching page starting at {params['start']}")
                        data = fetch_moex_data(url, params)
                        if data and data_block in data and data[data_block].get('data'):
                            insert_data_generic(conn, table_name, data[data_block], config, args.table)
                        else:
                            print(f"    No data or unexpected structure for page {params['start']} for ISIN {isin}.")

                        has_more = False
                        if data and f'{data_block}.cursor' in data:
                            cursor_data = data[f'{data_block}.cursor']['data']
                            if cursor_data and len(cursor_data) > 0:
                                index, total, pagesize = cursor_data[0]
                                if params['start'] + pagesize < total:
                                    params['start'] += pagesize
                                    has_more = True
                        if not data:
                            break

            else:
                 url = f"{base_url}/statistics/engines/stock/markets/bonds/bondization.json"
                 params = {'limit': default_limit, 'start': 0}
                 has_more = True
                 while has_more:
                     print(f"Fetching {args.table} data, page starting at {params['start']}")
                     data = fetch_moex_data(url, params)
                     if data and data_block in data and data[data_block].get('data'):
                         insert_data_generic(conn, table_name, data[data_block], config, args.table)
                     else:
                         print(f"  No data or unexpected structure for page {params['start']}.")

                     has_more = False
                     if data and f'{data_block}.cursor' in data:
                         cursor_data = data[f'{data_block}.cursor']['data']
                         if cursor_data and len(cursor_data) > 0:
                             index, total, pagesize = cursor_data[0]
                             if params['start'] + pagesize < total:
                                 params['start'] += pagesize
                                 has_more = True
                     if not data:
                         break

        else:
            print(f"Error: Unknown table type '{args.table}'.")

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()

