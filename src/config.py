import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

import numpy as np

from src.vault_env import load_vault_env

load_vault_env()


@dataclass(frozen=True)
class PathConfig:
    """Пути проекта."""

    BASEDIR = Path('/data/aturov/universal_control_group/')
    DATA_RAW_PATH: Path = BASEDIR / 'data/raw'
    DATA_PROCESSED_PATH: Path = BASEDIR / 'data/processed'
    CA_PATH = "/data/aturov/bee_skymobile_local_dmp_ca.crt"
    SQL_DIR = BASEDIR / "sql"
    SCRIPTS_DIR = BASEDIR / "scripts"
    SELECT_SLICE_SQL = SQL_DIR / "04_ukg_select_report_slice.sql"
    UKG_RUN_SCRIPT = SCRIPTS_DIR / "run_ukg_monthly.sh"


@dataclass
class DatabaseConfig:
    """Конфигурация подключений к базам данных."""

    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST")
    clickhouse_port: str = os.getenv("CLICKHOUSE_PORT")
    clickhouse_user: str = os.getenv("CLICKHOUSE_USER")
    clickhouse_password: str = os.getenv("CLICKHOUSE_PASSWORD")
    clickhouse_db: str = os.getenv("CLICKHOUSE_DB")
    clickhouse_protocol: str = os.getenv("CLICKHOUSE_PROTOCOL")
    CLICKHOUSE_CLUSTER: str = "edwh"
    ca_path: str = "/data/aturov/bee_skymobile_local_dmp_ca.crt"

    def __post_init__(self):
        ca_file = Path(self.ca_path)
        if ca_file.exists():
            self.ssl_args = {"ssl_ca": self.ca_path, "verify": self.ca_path}
        else:
            self.ssl_args = {"verify": True}

    @property
    def clickhouse_url(self) -> str:
        return (
            f"clickhouse://"
            f"{self.clickhouse_user}:{quote_plus(self.clickhouse_password)}@"
            f"{self.clickhouse_host}:{self.clickhouse_port}/"
            f"{self.clickhouse_db}?protocol={self.clickhouse_protocol}"
        )


@dataclass(frozen=True)
class UKGJobConfig:
    """Константы ежемесячного UKG job, кроме report_dt."""

    UKG_PCT: float = 0.10
    UKG_SALT: str = "ukg_global_holdout_v1"
    ASSIGNMENT_VERSION: int = 1
    DRY_RUN: bool = True
    SRM_ALPHA: float = 0.05
    KS_ALPHA: float = 0.05
    DEFAULT_SPLIT_VERSION: str = "core_v4_interconnect_arpu_traffic_mybeeline_py"
    ARPU_POSITIVE_BINS: tuple[float, ...] = (0.0, 10.0, 300.0, 500.0, 700.0, 1000.0, 2000.0, np.inf)
    ARPU_POSITIVE_LABELS: tuple[str, ...] = (
        "ARPU_0_10",
        "ARPU_10_300",
        "ARPU_300_500",
        "ARPU_500_700",
        "ARPU_700_1000",
        "ARPU_1000_2000",
        "ARPU_2000_PLUS",
    )
    TRAFFIC_POSITIVE_BINS: tuple[float, ...] = (0.0, 500.0, 2_000.0, 8_000.0, np.inf)
    TRAFFIC_POSITIVE_LABELS: tuple[str, ...] = ("TRAFFIC_LIGHT", "TRAFFIC_MID", "TRAFFIC_HEAVY", "TRAFFIC_TOP")
    CORE_STRATA_COLUMNS: tuple[str, ...] = (
        "ACTIVE_IND",
        "CUST_LEVEL",
        "REGION",
        "PERIODICITY",
        "FLAG_4G",
        "FLAG_ABONKA",
        "TENURE_BUCKET",
        "ARPU_BUCKET",
        "TRAFFIC_BUCKET",
        "MY_BEELINE_USER",
    )
    KS_COLUMNS: tuple[str, ...] = (
        "REVENUE_TOTAL_INTERCONNECT",
        "REVENUE_TOTAL",
        "TOTAL_RECHARGE",
        "BALANCE_END",
        "USAGE_INTERNET",
        "TOTAL_MOU",
        "LIFETIME_TOTAL",
        "DAYS_WITHOUT_PAYMENT",
    )
    CLI_DATE_FORMATS: tuple[str, ...] = ("%Y-%m-%d", "%Y-%m")


@dataclass(frozen=True)
class AnalysisConfig:
    """Пути и настройки для валидаций и артефактов анализа."""

    OUTPUT_DIR: Path = PathConfig.BASEDIR / "logs" / "ukg_analysis"
    VALIDATION_SQLS: tuple[Path, ...] = (
        PathConfig.SQL_DIR / "05_ukg_validation_inserted_rows.sql",
        PathConfig.SQL_DIR / "06_ukg_validation_global_summary.sql",
        PathConfig.SQL_DIR / "07_ukg_validation_group_summary.sql",
    )
    VALIDATION_NAMES: tuple[str, ...] = (
        "inserted_rows_for_report_dt",
        "global_assignment_summary",
        "group_summary",
    )


path_config = PathConfig()
database = DatabaseConfig()
ukg_job_config = UKGJobConfig()
analysis_config = AnalysisConfig()


