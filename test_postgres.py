import psycopg2
import random
import string
import time
import threading
from psycopg2 import OperationalError
from concurrent.futures import ThreadPoolExecutor

# Konfiguracja połączenia
PG_HOST = "localhost"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASSWORD = "postgres"
DB_NAME = "postgres"
SCHEMA_NAME = "test_schema"
RUN_DURATION = 30  # sekundy
INSERT_BATCH = 100

stop_event = threading.Event()

def check_connection():
    while not stop_event.is_set():
        try:
            with psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=DB_NAME,
                user=PG_USER, password=PG_PASSWORD,
                connect_timeout=3
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            print("[CONN CHECK] OK")
        except OperationalError as e:
            print(f"[CONN CHECK] ❌ {e}")
        time.sleep(2)

def insert_worker():
    i = 0
    while not stop_event.is_set():
        try:
            with psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=DB_NAME,
                user=PG_USER, password=PG_PASSWORD,
                connect_timeout=3
            ) as conn:
                with conn.cursor() as cur:
                    while not stop_event.is_set():
                        values = [("user_" + str(i + j)) for j in range(INSERT_BATCH)]
                        args_str = ",".join(cur.mogrify("(%s)", (v,)).decode() for v in values)
                        cur.execute(f"INSERT INTO {SCHEMA_NAME}.test_table (name) VALUES {args_str}")
                        conn.commit()
                        i += INSERT_BATCH
                        print(f"[INSERT] +{INSERT_BATCH} (total {i})")
                        time.sleep(0.1)
        except OperationalError as e:
            print(f"[INSERT] ❌ Połączenie utracone: {e}")
            time.sleep(2)

def select_worker():
    while not stop_event.is_set():
        try:
            with psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=DB_NAME,
                user=PG_USER, password=PG_PASSWORD,
                connect_timeout=3
            ) as conn:
                with conn.cursor() as cur:
                    while not stop_event.is_set():
                        rand_id = random.randint(1, 100_000)
                        cur.execute(f"SELECT name FROM {SCHEMA_NAME}.test_table WHERE id = %s", (rand_id,))
                        cur.fetchone()
                        print(f"[SELECT] id={rand_id}")
                        time.sleep(0.05)
        except OperationalError as e:
            print(f"[SELECT] ❌ Połączenie utracone: {e}")
            time.sleep(2)

def setup_schema():
    with psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=DB_NAME,
        user=PG_USER, password=PG_PASSWORD
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.test_table (
                    id SERIAL PRIMARY KEY,
                    name TEXT
                );
            """)
        conn.commit()
        print("✔️ Schemat i tabela przygotowane.")

def cleanup_schema():
    with psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=DB_NAME,
        user=PG_USER, password=PG_PASSWORD
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA I
