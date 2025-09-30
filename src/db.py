import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Any, Dict, List


def get_engine() -> Engine:
    host = os.getenv("MARIADB_HOST", "127.0.0.1")
    port = int(os.getenv("MARIADB_PORT", "3306"))
    user = os.getenv("MARIADB_USER", "app")
    password = os.getenv("MARIADB_PASSWORD", "apppw")
    db = os.getenv("MARIADB_DB", "openflights")

    url = f"mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url, pool_pre_ping=True, pool_recycle=3600)
    return engine


def run_query(sql: str, params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        rows = [dict(r._mapping) for r in result]
    return rows


def run_scalar(sql: str, params: Dict[str, Any] | None = None) -> Any:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        row = result.fetchone()
        return None if row is None else list(row._mapping.values())[0]
