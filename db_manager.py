import psycopg2


class DBManager:
    def __init__(self, dbname, user, password, host="localhost", port=5432):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def connect(self):
        """Устанавливает соединение с базой данных"""
        return psycopg2.connect(
            dbname=self.dbname, user=self.user, password=self.password,
            host=self.host, port=self.port
        )

    def execute_query(self, query, params=None):
        """Выполняет SQL-запрос к базе данных"""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchall()
            conn.commit()
        return result

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании"""
        query = """
            SELECT company_name, COUNT(vacancies_name) AS count_vacancies
            FROM employers JOIN vacancies USING (employer_id)
            GROUP BY employers.company_name
        """
        return self.execute_query(query)

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании, названия вакансии,
        зарплаты и ссылки на вакансию"""
        query = """
            SELECT employers.company_name, vacancies.vacancies_name,
            vacancies.payment, vacancies_url
            FROM employers JOIN vacancies USING (employer_id)
        """
        return self.execute_query(query)

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям"""
        query = "SELECT AVG(payment) AS avg_payment FROM vacancies"
        return self.execute_query(query)

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям"""
        query = """
            SELECT * FROM vacancies
            WHERE payment > (SELECT AVG(payment) FROM vacancies)
        """
        return self.execute_query(query)

    def get_vacancies_with_keyword(self, keyword):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        query = """
            SELECT * FROM vacancies
            WHERE lower(vacancies_name) LIKE %s
        """
        keyword = f'%{keyword.lower()}%'
        return self.execute_query(query, (keyword,))
