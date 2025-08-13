#!/usr/bin/env python3
import argparse
import logging
import os
import random
import signal
import string
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional

import psycopg2
from psycopg2 import OperationalError, InterfaceError, DatabaseError
from psycopg2.extras import RealDictCursor


def parse_args():
    p = argparse.ArgumentParser(
        description="PgPool failover tester: INSERT/SELECT + ciągłe sprawdzanie połączenia z autoreconnectem."
    )
    p.add_argument("--host", default=os.getenv("PGHOST", "127.0.0.1"), help="Host PgPool")
    p.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "9999")), help="Port PgPool")
    p.add_argument("--db", default=os.getenv("PGDATABASE", "postgres"), help="Nazwa bazy")
    p.add_argument("--user", default=os.getenv("PGUSER", "postgres"), help="Użytkownik")
    p.add_argument("--password", default=os.getenv("PGPASSWORD", ""), help="Hasło")
    p.add_argument("--table", default="failover_test", help="Nazwa tabeli testowej (zostanie DROP na końcu)")
    p.add_argument("--workers", type=int, default=4, help="Liczba wątków pracy (INSERT/SELECT)")
    p.add_argument("--duration", type=int, default=120, help="Czas testu w sekundach")
    p.add_argument("--insert_ratio", type=float, default=0.6, help="Prawdopodobieństwo wykonania INSERT (0..1)")
    p.add_argument("--sleep_min_ms", type=int, default=50, help="Minimalna przerwa między operacjami (ms)")
    p.add_argument("--sleep_max_ms", type=int, default=150, help="Maksymalna przerwa między operacjami (ms)")
    p.add_argument("--statement_timeout_ms", type=int, default=5000, help="statement_timeout dla sesji (ms)")
    p.add_argument("--log_level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
        datefmt="%H:%M:%S",
    )


@dataclass
class ConnParams:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    statement_timeout_ms: int


def connect(params: ConnParams):
    """Nawiązuje połączenie z sensownymi keepalive oraz krótkim connect_timeout."""
    logging.debug("Łączenie z PgPool...")
    conn = psycopg2.connect(
        host=params.host,
        port=params.port,
        dbname=params.dbname,
        user=params.user,
        password=params.password,
        connect_timeout=5,
        application_name="pgpool_failover_tester",
        keepalives=1,
        keepalives_idle=5,
        keepalives_interval=5,
        keepalives_count=3,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = %s;", (params.statement_timeout_ms,))
        cur.execute("SET client_min_messages = 'warning';")
    logging.debug("Połączenie ustanowione.")
    return conn


def is_connection_usable(conn) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            _ = cur.fetchone()
        return True
    except Exception:
        return False


class Reconnector:
    """Utrzymuje jedno połączenie na wątek; potrafi się przełączyć po błędzie."""
    def __init__(self, params: ConnParams):
        self.params = params
        self._conn = None
        self._lock = threading.Lock()

    def get(self):
        with self._lock:
            if self._conn is None or self._conn.closed or not is_connection_usable(self._conn):
                self._reconnect_locked()
            return self._conn

    def _reconnect_locked(self):
        # Zamknij stare
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        # Backoff z narastaniem przy kolejnych próbach
        delay = 0.5
        while True:
            try:
                self._conn = connect(self.params)
                logging.info("Połączenie odświeżone/ustanowione.")
                return
            except Exception as e:
                logging.warning(f"Reconnect nieudany: {e}. Próba ponownie za {delay:.1f}s")
                time.sleep(delay)
                delay = min(delay * 2, 5.0)


@dataclass
class Stats:
    inserts_ok: int = 0
    selects_ok: int = 0
    errors: int = 0
    reconnects: int = 0
    outages: int = 0
    last_outage_start: Optional[float] = None
    total_outage_seconds: float = 0.0

    def start_outage(self):
        if self.last_outage_start is None:
            self.last_outage_start = time.time()
            self.outages += 1

    def end_outage(self):
        if self.last_outage_start is not None:
            self.total_outage_seconds += time.time() - self.last_outage_start
            self.last_outage_start = None


def random_payload(n=24) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(n))


def ensure_table(conn, table: str):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {psql_ident(table)} (
        id BIGSERIAL PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL DEFAULT now(),
        payload TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS {psql_ident(table + "_ts_idx")} ON {psql_ident(table)} (ts DESC);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    logging.info(f"Przygotowano tabelę: {table}")


def drop_table(conn, table: str):
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {psql_ident(table)};")
    logging.info(f"Usunięto tabelę: {table}")


def psql_ident(name: str) -> str:
    # Proste, bezpieczne cytowanie identyfikatora
    return '"' + name.replace('"', '""') + '"'


def do_insert(conn, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO {psql_ident(table)} (payload) VALUES (%s);", (random_payload(),))


def do_select(conn, table: str) -> None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"SELECT count(*) AS cnt, max(ts) AS last_ts FROM {psql_ident(table)};")
        _ = cur.fetchone()


def worker_loop(name: str, reconnector: Reconnector, table: str, insert_ratio: float,
                sleep_min_ms: int, sleep_max_ms: int, stop_evt: threading.Event, stats: Stats):
    threading.current_thread().name = name
    while not stop_evt.is_set():
        conn = None
        try:
            conn = reconnector.get()
            if random.random() < insert_ratio:
                do_insert(conn, table)
                stats.inserts_ok += 1
            else:
                do_select(conn, table)
                stats.selects_ok += 1
            # Po udanej operacji zakończ ewentualny stan outage
            stats.end_outage()
        except (OperationalError, InterfaceError, DatabaseError) as e:
            stats.errors += 1
            logging.warning(f"Błąd operacji: {e}. Odświeżam połączenie.")
            stats.start_outage()
            # Wymuś reconnect na następną iterację
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            reconnector._reconnect_locked()
            stats.reconnects += 1
        except Exception as e:
            stats.errors += 1
            logging.exception(f"Nieoczekiwany błąd: {e}")
        # Losowa krótka pauza
        time.sleep(random.uniform(sleep_min_ms / 1000.0, sleep_max_ms / 1000.0))


def health_pinger(reconnector: Reconnector, stop_evt: threading.Event, stats: Stats):
    threading.current_thread().name = "health-ping"
    while not stop_evt.is_set():
        try:
            conn = reconnector.get()
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                _ = cur.fetchone()
            stats.end_outage()
        except Exception as e:
            logging.warning(f"Ping nieudany: {e}")
            stats.start_outage()
        time.sleep(1.0)


def main():
    args = parse_args()
    setup_logging(args.log_level)

    params = ConnParams(
        host=args.host,
        port=args.port,
        dbname=args.db,
        user=args.user,
        password=args.password,
        statement_timeout_ms=args.statement_timeout_ms,
    )

    # Globalne statystyki
    stats = Stats()
    stop_evt = threading.Event()

    # Graceful shutdown na Ctrl+C
    def handle_sigint(sig, frame):
        logging.info("Przerywam — proszę czekać na sprzątanie…")
        stop_evt.set()
    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    # Połączenie inicjalne do DDL
    base_conn = connect(params)
    try:
        ensure_table(base_conn, args.table)

        # Wątki robocze
        workers = []
        for i in range(args.workers):
            reconnector = Reconnector(params)
            t = threading.Thread(
                target=worker_loop,
                args=(f"worker-{i+1}", reconnector, args.table, args.insert_ratio,
                      args.sleep_min_ms, args.sleep_max_ms, stop_evt, stats),
                daemon=True,
            )
            workers.append(t)

        # Wątek pingu zdrowia
        ping_reconnector = Reconnector(params)
        ping_thread = threading.Thread(target=health_pinger, args=(ping_reconnector, stop_evt, stats), daemon=True)

        # Start
        for t in workers:
            t.start()
        ping_thread.start()

        # Czas trwania testu
        end_ts = time.time() + args.duration
        while time.time() < end_ts and not stop_evt.is_set():
            time.sleep(0.5)

        stop_evt.set()
        for t in workers:
            t.join(timeout=5)
        ping_thread.join(timeout=5)

        # Podsumowanie
        stats.end_outage()
        logging.info("=== PODSUMOWANIE TESTU ===")
        logging.info(f"Inserts OK: {stats.inserts_ok}")
        logging.info(f"Selects OK: {stats.selects_ok}")
        logging.info(f"Błędy (łącznie): {stats.errors}")
        logging.info(f"Reconnecty: {stats.reconnects}")
        logging.info(f"Liczba przerw w łączności (outages): {stats.outages}")
        logging.info(f"Łączny czas przerw: {stats.total_outage_seconds:.2f}s")

    finally:
        try:
            drop_table(base_conn, args.table)  # sprzątanie danych testowych
        except Exception as e:
            logging.warning(f"Nie udało się usunąć tabeli {args.table}: {e}")
        try:
            base_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
