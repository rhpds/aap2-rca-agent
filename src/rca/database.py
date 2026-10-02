"""Database access shared by analysis and batch RCA code."""

from __future__ import annotations

import sys
from contextlib import contextmanager
from collections.abc import Mapping
from typing import Any

import psycopg2
import psycopg2.extras
import psycopg2.sql

TICKET_CLOSED_GRACE_HOURS = 4
_TICKET_COLUMNS = ("ticket_link", "ticket_resolve_datetime_gmt")

# Probe each table at most once per process.
_ticket_columns_present: dict[str, bool] = {}


def connect_db(config: Mapping[str, Any], *, use_dict_cursor: bool = False) -> Any:
    """Connect using the normalized mapping returned by load_database_config()."""
    kwargs: dict[str, Any] = {
        "host": config["host"],
        "port": config["port"],
        "dbname": config["name"],
        "user": config["user"],
        "password": config["password"],
    }
    if use_dict_cursor:
        kwargs["cursor_factory"] = psycopg2.extras.RealDictCursor
    return psycopg2.connect(**kwargs)


@contextmanager
def pooled_connection(pool: Any):
    """Borrow one connection from a psycopg2 pool and return it cleanly.

    Batch queries share a ``ThreadedConnectionPool``. A borrowed connection is
    never shared between concurrent workers, and any read-only transaction left
    open by a helper is rolled back before the connection is returned.
    """
    conn = pool.getconn()
    try:
        yield conn
    except BaseException:
        _return_pooled_connection(pool, conn)
        raise
    else:
        _return_pooled_connection(pool, conn)


def _return_pooled_connection(pool: Any, conn: Any) -> None:
    close = bool(getattr(conn, "closed", False))
    if not close:
        try:
            conn.rollback()
        except Exception:
            close = True
    pool.putconn(conn, close=close)


def lookup_job_bastion_row(
    config: Mapping[str, Any], job_id: str, *, conn: Any | None = None
) -> dict[str, Any] | None:
    """Fetch the source event's cluster-to-bastion mapping for one AAP job."""
    query = psycopg2.sql.SQL(
        "SELECT e.job_id, e.cluster_name, u.bastion_hostname, u.bastion_ssh_port, "
        "u.instance_base_path "
        "FROM {} e "
        "LEFT JOIN {} u ON e.cluster_name = u.cluster_name "
        "WHERE e.job_id = %s "
        "ORDER BY e.job_started DESC "
        "LIMIT 1"
    ).format(
        psycopg2.sql.Identifier(config["source_table"]),
        psycopg2.sql.Identifier(config["bastion_table"]),
    )

    owns_connection = conn is None
    if owns_connection:
        conn = connect_db(config, use_dict_cursor=True)
    try:
        with conn.cursor() as cur:
            cur.execute(query, (job_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        if owns_connection:
            conn.close()


def _has_ticket_columns(conn: Any, table: str) -> bool:
    if table not in _ticket_columns_present:
        with conn.cursor(cursor_factory=psycopg2.extensions.cursor) as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = %s AND column_name = ANY(%s)",
                (table, list(_TICKET_COLUMNS)),
            )
            found = {row[0] for row in cur.fetchall()}
        present = set(_TICKET_COLUMNS) <= found
        if not present:
            print(
                f"[WARN] {table} is missing {', '.join(_TICKET_COLUMNS)}; "
                "known-issue ticket filtering disabled, treating all known issues as active",
                file=sys.stderr,
            )
        _ticket_columns_present[table] = present
    return _ticket_columns_present[table]


def known_issue_active_sql(
    conn: Any,
    alias: str = "",
    *,
    table: str,
) -> psycopg2.sql.Composable:
    """Build the known-issue activity predicate used by batch matching queries.

    A row remains active when it has no linked ticket, no known resolution time,
    or its ticket was resolved less than ``TICKET_CLOSED_GRACE_HOURS`` ago. If
    the table lacks the ticket columns, filtering is disabled for that table.
    """

    def col(name: str) -> psycopg2.sql.Identifier:
        return psycopg2.sql.Identifier(alias, name) if alias else psycopg2.sql.Identifier(name)

    if not table:
        raise ValueError("table is required for known_issue_active_sql")

    if not _has_ticket_columns(conn, table):
        return psycopg2.sql.SQL("TRUE")

    return psycopg2.sql.SQL(
        "({tl} IS NULL OR {rd} IS NULL OR {rd} > NOW() - make_interval(hours => {h}))"
    ).format(
        tl=col("ticket_link"),
        rd=col("ticket_resolve_datetime_gmt"),
        h=psycopg2.sql.Literal(TICKET_CLOSED_GRACE_HOURS),
    )
