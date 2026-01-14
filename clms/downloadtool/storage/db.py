"""PostgreSQL storage for download tool tasks."""
import json
import os
from datetime import datetime, timezone

TABLE_NAME = "downloadtool_tasks"


def _get_psycopg2():
    try:
        import psycopg2
        from psycopg2 import extras
    except ImportError as exc:
        raise RuntimeError(
            "psycopg2 is required for downloadtool DB storage: {0}".format(exc)
        )
    return psycopg2, extras


def _get_dsn():
    dsn = os.environ.get("DOWNLOADTOOL_DB_DSN", "").strip()
    if not dsn:
        raise RuntimeError("DOWNLOADTOOL_DB_DSN is not set")
    return dsn


def _parse_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        cleaned = value.replace("Z", "+00:00") if value.endswith("Z") else value
        try:
            return datetime.fromisoformat(cleaned)
        except ValueError:
            return None
    return None


def _now_utc():
    return datetime.now(timezone.utc)


def _task_columns(payload):
    return {
        "user_id": payload.get("UserID"),
        "status": payload.get("Status"),
        "cdse_task_group_id": payload.get("cdse_task_group_id"),
        "registration_datetime": _parse_datetime(
            payload.get("RegistrationDateTime")
        ),
    }


class DownloadtoolRepository:
    """DB access layer for download tool tasks."""

    def __init__(self, dsn=None):
        self.dsn = dsn or _get_dsn()
        self.psycopg2, self.extras = _get_psycopg2()

    def _connect(self):
        conn = self.psycopg2.connect(self.dsn)
        self.extras.register_default_jsonb(conn, loads=json.loads)
        return conn

    def insert_task(self, task_id, payload):
        columns = _task_columns(payload)
        now = _now_utc()
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO {table} (
                        task_id,
                        payload,
                        user_id,
                        status,
                        cdse_task_group_id,
                        registration_datetime,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (task_id) DO NOTHING
                    RETURNING task_id
                    """.format(table=TABLE_NAME),
                    (
                        str(task_id),
                        self.extras.Json(payload),
                        columns["user_id"],
                        columns["status"],
                        columns["cdse_task_group_id"],
                        columns["registration_datetime"],
                        now,
                    ),
                )
                return cursor.fetchone() is not None

    def get_task(self, task_id):
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT payload FROM {table} WHERE task_id = %s".format(
                        table=TABLE_NAME
                    ),
                    (str(task_id),),
                )
                row = cursor.fetchone()
                return row[0] if row else None

    def search_tasks(self, user_id, status=None):
        params = [str(user_id)]
        where = ["user_id = %s"]
        if status is not None:
            where.append("status = %s")
            params.append(status)
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT task_id, payload FROM {table} WHERE {where}".format(
                        table=TABLE_NAME, where=" AND ".join(where)
                    ),
                    tuple(params),
                )
                return cursor.fetchall()

    def inspect_tasks(self, query=None):
        params = []
        where = ""
        if query:
            conditions = []
            for key, value in query.items():
                conditions.append("payload->>%s = %s")
                params.extend([key, str(value)])
            where = " WHERE {conds}".format(conds=" OR ".join(conditions))
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT task_id, payload FROM {table}{where} "
                    "ORDER BY task_id".format(
                        table=TABLE_NAME, where=where
                    ),
                    tuple(params),
                )
                return cursor.fetchall()

    def update_task(self, task_id, updates, status=None):
        now = _now_utc()
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE {table}
                    SET payload = payload || %s::jsonb,
                        status = COALESCE(%s, status),
                        updated_at = %s
                    WHERE task_id = %s
                    RETURNING payload
                    """.format(table=TABLE_NAME),
                    (self.extras.Json(updates), status, now, str(task_id)),
                )
                row = cursor.fetchone()
                return row[0] if row else None

    def delete_task(self, task_id):
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM {table} WHERE task_id = %s".format(
                        table=TABLE_NAME
                    ),
                    (str(task_id),),
                )
                return cursor.rowcount > 0

    def delete_all(self):
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM {table}".format(table=TABLE_NAME))
                return cursor.rowcount

    def has_tasks(self):
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM {table} LIMIT 1".format(table=TABLE_NAME)
                )
                return cursor.fetchone() is not None
