import os
from dataclasses import dataclass
from urllib.parse import quote_plus
from pathlib import Path
from src.vault_env import load_vault_env
load_vault_env()

class PathConfig:
    """
    Пути к ресурсам и внешним сервисам.
    """
    BASEDIR = Path('/data/aturov/universal_control_group/')
    data_raw_path: Path = BASEDIR / 'data/raw'
    data_processed_path: Path = BASEDIR / 'data/processed'
    data_final_path: Path = BASEDIR / 'data/final'
    CA_PATH = "/data/aturov/bee_skymobile_local_dmp_ca.crt"
    

#######################
# DATABASE CONFIG     #
#######################

@dataclass
class DatabaseConfig:
    """Конфигурация подключений к базам данных."""
    
    # ClickHouse параметры
    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST")
    clickhouse_port: str = os.getenv("CLICKHOUSE_PORT")
    clickhouse_user: str = os.getenv("CLICKHOUSE_USER")
    clickhouse_password: str = os.getenv("CLICKHOUSE_PASSWORD")
    clickhouse_db: str = os.getenv("CLICKHOUSE_DB")
    clickhouse_protocol: str = os.getenv("CLICKHOUSE_PROTOCOL")  # 'http' or 'https'
    CLICKHOUSE_CLUSTER: str = "edwh"
    # SSL настройки
    ca_path: str = "/data/aturov/bee_skymobile_local_dmp_ca.crt"
    
    def __post_init__(self):
        """Инициализация после создания объекта."""
        ca_file = Path(self.ca_path)
        if ca_file.exists():
            self.ssl_args = {"ssl_ca": self.ca_path, "verify": self.ca_path}
        else:
            self.ssl_args = {"verify": True}

    
    
    @property
    def clickhouse_url(self) -> str:
        """URL подключения к ClickHouse."""
        # ИСПРАВЛЕНО: формат как в рабочем примере
        url = (f"clickhouse://"
               f"{self.clickhouse_user}:{quote_plus(self.clickhouse_password)}@"
               f"{self.clickhouse_host}:{self.clickhouse_port}/"
               f"{self.clickhouse_db}?protocol={self.clickhouse_protocol}")
        return url
    
    
# Инстансы для импорта из проекта
path_config = PathConfig()
database = DatabaseConfig()
