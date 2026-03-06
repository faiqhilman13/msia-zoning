from __future__ import annotations

from pathlib import Path

import psycopg


def apply_sql_file(conn: psycopg.Connection, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()
