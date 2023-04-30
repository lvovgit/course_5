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
                            'url text, '
                            'salary int)')
        conn.close()


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


    def get_companies_and_vacancies_count(self) -> list:
        """Получает список всех компаний и количество вакансий у каждой компании"""
        result = self._execute_query("SELECT employer_name, COUNT(*) as quantity_vacancies "
                                     "FROM vacancies "
                                     "LEFT JOIN employers USING(employer_id)"
                                     "GROUP BY employer_name "
                                     "ORDER BY quantity_vacancies DESC, employer_name")
        return result


    def get_all_vacancies(self) -> list:
        """ Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию"""
        result = self._execute_query("SELECT employers.employer_name, vacancy_name, salary, url "
                                     "FROM vacancies "
                                     "JOIN employers USING(employer_id)"
                                     "WHERE salary IS NOT NULL "
                                     "ORDER BY salary DESC, vacancy_name")
        return result


    def get_avg_salary(self) -> list:
        """ Получает среднюю зарплату по вакансиям"""
        result = self._execute_query("SELECT ROUND(AVG(salary)) as average_salary "
                                     "FROM vacancies")
        return result


    def get_vacancies_with_higher_salary(self) -> list:
        """ Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям"""
        result = self._execute_query("SELECT vacancy_name, salary "
                                     "FROM vacancies "
                                     "WHERE salary > (SELECT AVG(salary) FROM vacancies) "
                                     "ORDER BY salary DESC, vacancy_name")
        return result


    def get_vacancies_with_keyword(self, word: str) -> list:
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например “python”"""
        result = self._execute_query("SELECT vacancy_name "
                                     "FROM vacancies "
                                     f"WHERE vacancy_name ILIKE '%{word}%'"
                                     "ORDER BY vacancy_name")
        return result
