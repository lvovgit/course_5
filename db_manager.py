import psycopg2
from hh_parser import HeadHunter
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
            cur.execute(f"DROP DATABASE IF EXISTS {self.dbname}")  # затирает БД если она уже существует
            cur.execute(f"CREATE DATABASE {self.dbname}")  # создает новую БД
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
        with conn:
            with conn.cursor() as cur:
                cur.execute('CREATE TABLE IF NOT EXISTS employers '
                            '('
                            'employer_id int PRIMARY KEY, '
                            'employer_name varchar(255) UNIQUE NOT NULL)')
                cur.execute('CREATE TABLE IF NOT EXISTS vacancies '
                            '('
                            'vacancy_id int PRIMARY KEY, '
                            'vacancy_name varchar(255) NOT NULL, '
                            'employer_id int REFERENCES employers(employer_id) NOT NULL, '
                            'city varchar(255), '
                            'salary_min int,'
                            'salary_max int,'
                            'url text)')
        conn.close()

    def insert_data_into_db(self, data):
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        with conn:
            with conn.cursor() as cur:
                conn.autocommit = True
                cur = conn.cursor()
                for item in data['items']:
                    employer = item['employer']
                    cur.execute("""
                        INSERT INTO employers (employer_id, employer_name)
                        VALUES (%s, %s)
                        ON CONFLICT (employer_id) DO NOTHING;
                    """, (employer['id'], employer['name']))
                    conn.commit()

                    if 'salary' in data.keys():
                        if data.get('salary') is not None:
                            if 'from' in data.get('salary'):
                                salary = data.get('salary').get('from')
                            else:
                                salary = None
                        else:
                            salary = None
                    else:
                        salary = None
                    cur.execute("""
                        INSERT INTO vacancies (vacancy_id, vacancy_name, employer_id, city, salary_min, salary_max, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (vacancy_id) DO NOTHING;
                    """, (item['id'], item['name'], employer['id'], item['address'], salary, salary, item['url']))
                    conn.commit()

    def insert(self, table: str, data: list) -> None:
        """Добавление данных в базу данных в зависимости от таблицы"""
        conn = psycopg2.connect(dbname=self.dbname, **self.params)

        with conn:
            with conn.cursor() as cur:
                if table == 'employers':
                    cur.executemany('INSERT INTO employers(employer_id, employer_name) '
                                    'VALUES(%s, %s)', data)
                elif table == 'vacancies':
                    cur.executemany('INSERT INTO vacancies (vacancy_id, vacancy_name, employer_id, '
                                    'city, salary, url) '
                                    'VALUES(%s, %s, %s, %s, %s, %s)'
                                    'ON CONFLICT (vacancy_id) DO NOTHING', data)

        conn.close()

    def _execute_query(self, query) -> list:
        """Возвращает результат запроса"""
        conn = psycopg2.connect(dbname=self.dbname, **self.params)

        with conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall()

        conn.close()

        return result


def insert_data_into_db(self, data, db):
    cur = db.conn.cursor()
    for item in data['items']:
        employer = item['employer']
        cur.execute("""
                INSERT INTO companies (name, city, address, description)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name) DO NOTHING;
            """, (employer['name'], employer['area']['name'], employer['address'], employer['description']))
        db.conn.commit()

        cur.execute("""
                INSERT INTO vacancies (name, company_id, salary_min, salary_max, url)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING;
            """, (item['name'], employer['id'], item['salary']['from'], item['salary']['to'], item['url']))
        db.conn.commit()


if __name__ == '__main__':
    search_keyword = 'Python'
    hh = HeadHunter(search_keyword)
    k = hh.get_request()