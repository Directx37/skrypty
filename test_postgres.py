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
PG_BASE_DB = "postgres"
CONNECT_TIMEOUT = 3

# Testowa baza i tabela
TEST_DB_NAME = "test_db_" + ''.join(random.choices(string.ascii_lowercase, k=6))
RUN_DURATION = 30  # czas testu w sekundach
INSERT_BATCH = 100

# Flaga stopu dla wszystkich wątków
stop_event = threading.Event()

def check_connection():
    while not stop_event.is_set():
        try:
            with psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=TEST_DB_NAME,
                user=PG_USER, password=PG_PASSWORD,
                connect_timeout=CONNECT_TIMEOUT
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            print("[CONN CHECK] OK")
        except OperationalError as e:
            print(f"[CONN CHECK] ❌ {e}")
        time.sleep(2)

def insert_worker():
    with psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=TEST_DB_NAME,
        user=PG_USER, password=PG_PASSWORD,
        connect_timeout=CONNECT_TIMEOUT
    ) as conn:
        with conn.cursor() as cur:
            i = 0
            while not stop_event.is_set():
                values = [("user_" + str(i + j)) for j in range(INSERT_BATCH)]
                args_str = ",".join(cur.mogrify("(%s)", (v,)).decode() for v in values)
                cur.execute(f"INSERT INTO test_table (name) VALUES {args_str}")
                conn.commit()
                i += INSERT_BATCH
                print(f"[INSERT] +{INSERT_BATCH} (total {i})")
                time.sleep(0.1)

def select_worker():
    with psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=TEST_DB_NAME,
        user=PG_USER, password=PG_PASSWORD,
        connect_timeout=CONNECT_TIMEOUT
    ) as conn:
        with conn.cursor() as cur:
            while not stop_event.is_set():
                rand_id = random.randint(1, 100_000)
                cur.execute("SELECT name FROM test_table WHERE id = %s", (rand_id,))
                cur.fetchone()
                print(f"[SELECT] id={rand_id}")
                time.sleep(0.05)

def create_test_db():
    with psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_BASE_DB,
        user=PG_USER, password=PG_PASSWORD
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {TEST_DB_NAME};")

def drop_test_db():
    with psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_BASE_DB,
        user=PG_USER, password=PG_PASSWORD
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid();
            """, (TEST_DB_NAME,))
            time.sleep(1)
            cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME};")

def init_test_table():
    with psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=TEST_DB_NAME,
        user=PG_USER, password=PG_PASSWORD
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE test_table (id SERIAL PRIMARY KEY, name TEXT);")
        conn.commit()

if __name__ == "__main__":
    try:
        print(f"▶️ Tworzenie testowej bazy: {TEST_DB_NAME}")
        create_test_db()
        init_test_table()

        print(f"▶️ Uruchamianie testu na {RUN_DURATION} sekund...")
        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(insert_worker)
            executor.submit(select_worker)
            executor.submit(check_connection)

            time.sleep(RUN_DURATION)
            stop_event.set()
            print("⏹️ Zatrzymywanie wątków...")

        print("✅ Test zakończony")

    finally:
        print("🧹 Usuwanie bazy testowej...")
        drop_test_db()
