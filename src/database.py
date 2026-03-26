from __future__ import annotations

from pathlib import Path
from typing import Any

import clickhouse_connect
import pandas as pd
import urllib3
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.base_models import UkgAssignment
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


def get_clickhouse_client() -> clickhouse_connect.driver.client.Client:
    if clickhouse_client is None:
        raise RuntimeError("ClickHouse client не создан")
    return clickhouse_client


def get_clickhouse_engine():
    if clickhouse_engine is None:
        raise RuntimeError("ClickHouse engine не создан")
    return clickhouse_engine


def get_clickhouse_session():
    if clickhouse_session_factory is None:
        raise RuntimeError("ClickHouse session factory не создан")
    return clickhouse_session_factory()


def read_sql_file(sql_path: str | Path) -> str:
    path = Path(sql_path)
    if not path.is_absolute():
        path = path_config.BASEDIR / path
    return path.read_text(encoding="utf-8")


def query_df(sql_path: str | Path, **query_params: Any) -> pd.DataFrame:
    """Читает SQL из файла и возвращает результат как DataFrame."""
    sql = read_sql_file(sql_path)
    engine = get_clickhouse_engine()
    with engine.connect() as connection:
        return pd.read_sql_query(text(sql), connection, params=query_params)


def insert_dataframe(table_name: str, dataframe: pd.DataFrame) -> None:
    if dataframe.empty:
        return
    client = get_clickhouse_client()
    client.insert_df(table=table_name, df=dataframe)


class UKGAssignmentRepository:
    """Подготовка и вставка новых assignment-строк в ClickHouse через ORM-модель UkgAssignment."""

    @staticmethod
    def _model_to_dict(model: UkgAssignment) -> dict[str, Any]:
        return {
            "subs_id": model.subs_id,
            "first_seen_dt": model.first_seen_dt,
            "assignment_dt": model.assignment_dt,
            "experiment_group": model.experiment_group,
            "is_control": model.is_control,
            "split_hash": model.split_hash,
            "ukg_pct": model.ukg_pct,
            "ukg_salt": model.ukg_salt,
            "assignment_version": model.assignment_version,
            "created_at": model.created_at,
        }

    def build_models(
        self,
        new_assignment_df: pd.DataFrame,
        report_dt: Any,
        ukg_pct: float,
        ukg_salt: str,
        assignment_version: int,
    ) -> list[UkgAssignment]:
        if new_assignment_df.empty:
            return []

        # Сначала приводим типы через DataFrame, затем создаем ORM-объекты.
        created_at = pd.Timestamp.utcnow().tz_localize(None).to_pydatetime()
        prepared = pd.DataFrame(
            {
                "subs_id": pd.to_numeric(new_assignment_df["SUBS_ID"], errors="coerce").astype("uint64"),
                "first_seen_dt": report_dt,
                "assignment_dt": report_dt,
                "experiment_group": new_assignment_df["experiment_group"].astype("string"),
                "is_control": new_assignment_df["is_control"].astype("uint8"),
                "split_hash": new_assignment_df["split_hash"].astype("uint64"),
                "ukg_pct": float(ukg_pct),
                "ukg_salt": str(ukg_salt),
                "assignment_version": int(assignment_version),
                "created_at": created_at,
            }
        )
        return [UkgAssignment(**row) for row in prepared.to_dict(orient="records")]

    def insert_rows(self, models: list[UkgAssignment]) -> int:
        if not models:
            return 0

        try:
            with get_clickhouse_session() as session:
                session.add_all(models)
                session.commit()
            return len(models)
        except Exception:
            logger.exception("ORM insert в ukg_assignment не удался, fallback на clickhouse client insert_df")
            insert_dataframe(
                UkgAssignment.__table__.fullname,
                pd.DataFrame([self._model_to_dict(model) for model in models]),
            )
            return len(models)


def test_connections():
    """Короткая проверка, что ClickHouse engine и client доступны."""
    results = {}

    for name, engine in [("clickhouse", clickhouse_engine)]:
        if engine:
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                results[name] = "✅"
            except Exception as e:
                results[name] = f"❌ {str(e)[:50]}..."
        else:
            results[name] = "❌ не создан"

    if clickhouse_client:
        try:
            clickhouse_client.command("SELECT 1")
            results["clickhouse_client"] = "✅"
        except Exception as e:
            results["clickhouse_client"] = f'❌ {str(e)[:50]}...'
    else:
        results["clickhouse_client"] = '❌ не создан'

    return results

if __name__ == "__main__":
    logger.info("Тест подключений:")
    results = test_connections()
    for db, status in results.items():
        logger.info(f"{db}: {status}")