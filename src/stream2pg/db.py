from __future__ import annotations
from datetime import datetime
from typing import Any

import psycopg2
from psycopg2 import sql


def infer_sql_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE PRECISION"
    if isinstance(value, str):
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return "TIMESTAMP"
        except Exception:
            return "TEXT"
    return "TEXT"


def ensure_table(conn: psycopg2.extensions.connection, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {} (id BIGSERIAL PRIMARY KEY)").format(
                sql.Identifier(table_name)
            )
        )


def ensure_columns(
    conn: psycopg2.extensions.connection, table_name: str, record: dict
) -> None:
    with conn.cursor() as cur:
        for key, value in record.items():
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = %s
                    AND column_name = %s
                )
                """,
                (table_name, key),
            )
            exists = cur.fetchone()[0]
            if not exists:
                cur.execute(
                    sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                        sql.Identifier(table_name),
                        sql.Identifier(key),
                        sql.SQL(infer_sql_type(value)),
                    )
                )


def convert_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return value
    return value


def insert_record(
    conn: psycopg2.extensions.connection, table_name: str, record: dict
) -> None:
    columns = list(record.keys())
    values = [convert_value(record[c]) for c in columns]
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
            ),
            values,
        )
