from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base


from src.config import database
from src.logger import logger
import urllib3

# Отключаем SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- ENGINES ---
clickhouse_engine = None

# ClickHouse с SSL параметрами
try:
    clickhouse_engine = create_engine(
        database.clickhouse_url,
        connect_args=database.ssl_args
    )
    logger.info("✅ ClickHouse engine создан")
except Exception as e:
    logger.error(f"❌ ClickHouse: {e}")

# --- SESSIONS ---
clickhouse_session_factory = sessionmaker(bind=clickhouse_engine) if clickhouse_engine else None

# Base для моделей
Base = declarative_base()

# --- SIMPLE TEST ---
def test_connections():
    """Простой тест подключений."""
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
    
    return results

# --- TEST SCRIPT ---
if __name__ == "__main__":
    logger.info("Тест подключений:")
    results = test_connections()
    for db, status in results.items():
        logger.info(f"{db}: {status}")