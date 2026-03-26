from __future__ import annotations

from pathlib import Path
from typing import Any

import clickhouse_connect
import pandas as pd
import urllib3
from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker

from src.config import database, path_config
from src.logger import logger

# Отключаем SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- ENGINES ---
clickhouse_engine = None
clickhouse_client = None


def _build_clickhouse_client() -> clickhouse_connect.driver.client.Client:
    secure = str(database.clickhouse_protocol).lower() == "https"
    verify: str | bool = database.ca_path if Path(database.ca_path).exists() else False
    ca_cert = database.ca_path if Path(database.ca_path).exists() else None
    return clickhouse_connect.get_client(
        host=database.clickhouse_host,
        port=int(database.clickhouse_port),
        username=database.clickhouse_user,
        password=database.clickhouse_password,
        database=database.clickhouse_db,
        secure=secure,
        verify=verify,
        ca_cert=ca_cert,
    )

# ClickHouse с SSL параметрами
try:
    clickhouse_engine = create_engine(
        database.clickhouse_url,
        connect_args=database.ssl_args
    )
    logger.info("✅ ClickHouse engine создан")
except Exception as e:
    logger.error(f"❌ ClickHouse: {e}")

try:
    clickhouse_client = _build_clickhouse_client()
    logger.info("✅ ClickHouse client создан")
except Exception as e:
    logger.error(f"❌ ClickHouse client: {e}")

# --- SESSIONS ---
clickhouse_session_factory = sessionmaker(bind=clickhouse_engine) if clickhouse_engine else None

# Base для моделей
Base = declarative_base()


def get_clickhouse_client() -> clickhouse_connect.driver.client.Client:
    if clickhouse_client is None:
        raise RuntimeError("ClickHouse client не создан")
    return clickhouse_client


def get_clickhouse_engine():
    if clickhouse_engine is None:
        raise RuntimeError("ClickHouse engine не создан")
    return clickhouse_engine


def read_sql_file(sql_path: str | Path) -> str:
    path = Path(sql_path)
    if not path.is_absolute():
        path = path_config.BASEDIR / path
    return path.read_text(encoding="utf-8")


def render_sql(sql_path: str | Path, **format_params: Any) -> str:
    sql = read_sql_file(sql_path)
    return sql.format(**format_params) if format_params else sql


def split_sql_statements(sql_script: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False

    i = 0
    n = len(sql_script)
    while i < n:
        char = sql_script[i]
        nxt = sql_script[i + 1] if i + 1 < n else ""

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
                buffer.append(char)
            i += 1
            continue

        if in_block_comment:
            if char == "*" and nxt == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        if not in_single_quote and not in_double_quote:
            if char == "-" and nxt == "-":
                in_line_comment = True
                i += 2
                continue
            if char == "/" and nxt == "*":
                in_block_comment = True
                i += 2
                continue

        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            buffer.append(char)
            i += 1
            continue

        if char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            buffer.append(char)
            i += 1
            continue

        if char == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            i += 1
            continue

        buffer.append(char)
        i += 1

    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)
    return statements


def execute_sql_file(sql_path: str | Path, **format_params: Any) -> None:
    client = get_clickhouse_client()
    sql_script = render_sql(sql_path, **format_params)
    for statement in split_sql_statements(sql_script):
        client.command(statement)


def query_df(sql_path: str | Path, **query_params: Any) -> pd.DataFrame:
    '''Выполняет SQL запрос из файла и возвращает результат в виде DataFrame.'''
    sql = read_sql_file(sql_path)
    engine = get_clickhouse_engine()
    with engine.connect() as connection:
        return pd.read_sql_query(text(sql), connection, params=query_params)


def query_df_from_sql(sql: str, **query_params: Any) -> pd.DataFrame:
    engine = get_clickhouse_engine()
    with engine.connect() as connection:
        return pd.read_sql_query(text(sql), connection, params=query_params)


def query_multiple_dataframes(sql_path: str | Path, **query_params: Any) -> list[pd.DataFrame]:
    '''Выполняет SQL запрос из файла, который может содержать несколько SQL операторов, и возвращает результат всех SELECT операторов в виде списка DataFrame.'''
    sql_script = read_sql_file(sql_path)
    engine = get_clickhouse_engine()
    result: list[pd.DataFrame] = []
    with engine.connect() as connection:
        for statement in split_sql_statements(sql_script):
            if statement.lstrip().upper().startswith("SELECT"):
                result.append(pd.read_sql_query(text(statement), connection, params=query_params))
            else:
                connection.execute(text(statement), query_params)
    return result


def insert_dataframe(table_name: str, dataframe: pd.DataFrame) -> None:
    if dataframe.empty:
        return
    client = get_clickhouse_client()
    client.insert_df(table=table_name, df=dataframe)


def ensure_ukg_tables() -> None:
    '''Создает необходимые таблицы в ClickHouse, если их нет.'''
    execute_sql_file(
        path_config.BASEDIR / "sql" / "03_ukg_assignment_monthly.sql",
        cluster=database.CLICKHOUSE_CLUSTER,
        database_name=database.clickhouse_db,
    )

# --- SIMPLE TEST ---
def test_connections():
    results = {}
    
    for name, engine in [('clickhouse', clickhouse_engine)
                        ]:
        if engine:
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                results[name] = '✅'
            except Exception as e:
                results[name] = f'❌ {str(e)[:50]}...'
        else:
            results[name] = '❌ не создан'

    if clickhouse_client:
        try:
            clickhouse_client.command("SELECT 1")
            results["clickhouse_client"] = '✅'
        except Exception as e:
            results["clickhouse_client"] = f'❌ {str(e)[:50]}...'
    else:
        results["clickhouse_client"] = '❌ не создан'
    
    return results

# --- TEST SCRIPT ---
if __name__ == "__main__":
    logger.info("Тест подключений:")
    results = test_connections()
    for db, status in results.items():
        logger.info(f"{db}: {status}")