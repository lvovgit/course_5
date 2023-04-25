import psycopg2
from psycopg2 import errors

class DBManager:
    def __init__(self, dbname, params):
        self.dbname = dbname
        self.params = params

    def create_database(self):
        conn = psycopg2.connect(dbname='postgres', **self.params)
        conn.autocommit = True
        cur = conn.cursor()

        try:
            cur.execute(f"DROP DATABASE IF EXISTS {self.dbname}") # затирает БД если она уже существует
            cur.execute(f"CREATE DATABASE {self.dbname}") # создает новую БД
        except psycopg2.errors.ObjectInUse:
            cur.execute("SELECT pg_terminate_backend(pg_stat_activity.pid) "
                        "FROM pg_stat_activity "
                        f"WHERE pg_stat_activity.datname = '{self.dbname}' ")
            cur.execute(f"DROP DATABASE IF EXISTS {self.dbname}")
            cur.execute(f"CREATE DATABASE {self.dbname}")

        finally:
            cur.close()
            conn.close()

        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        with conn.cursor() as cur:
            cur.execute('...')
            cur.execute('...')
        conn.close()

