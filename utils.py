import psycopg2
import requests


def get_vacancies(employer_id):
    """Получение данных о вакансиях от конкретного работодателя с помощью API hh.ru"""
    try:
        params = {
            'area': 1,
            'page': 0,
            'per_page': 10
        }
        url = f"https://api.hh.ru/vacancies?employer_id={employer_id}"
        data_vacancies = requests.get(url, params=params).json()

        vacancies_data = []
        for item in data_vacancies["items"]:
            hh_vacancies = {
                'vacancy_id': int(item['id']),
                'vacancies_name': item['name'],
                'payment': item["salary"]["from"] if item["salary"] else None,
                'requirement': item['snippet']['requirement'],
                'vacancies_url': item['alternate_url'],
                'employer_id': employer_id
            }
            if hh_vacancies['payment'] is not None:
                vacancies_data.append(hh_vacancies)
        return vacancies_data
    except (requests.exceptions.RequestException, KeyError) as e:
        print(f"Произошла ошибка при добавлении данных в таблицы: {e}")
        return []


def get_employer(employer_id):
    """Получение данных о работодателе по его идентификатору с помощью API hh.ru"""
    try:
        url = f"https://api.hh.ru/employers/{employer_id}"
        data_vacancies = requests.get(url).json()
        hh_company = {
            "employer_id": int(employer_id),
            "company_name": data_vacancies['name'],
            "open_vacancies": data_vacancies['open_vacancies']
        }
        return hh_company
    except (requests.exceptions.RequestException, KeyError) as e:
        print(f"Произошла ошибка при добавлении данных в таблицы: {e}")
        return None


def create_table():
    """Создание базы данных, создание таблиц"""
    try:
        # Подключение к базе данных postgres
        conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password="Smash2012")
        conn.autocommit = True
        cur = conn.cursor()

        # Удаление базы данных coursework_5, если она существует
        cur.execute("DROP DATABASE IF EXISTS coursework_5")

        # Создание базы данных coursework_5
        cur.execute("CREATE DATABASE coursework_5")

        # Закрытие соединения с базой данных postgres
        conn.close()

        # Подключение к новой базе данных coursework_5
        conn = psycopg2.connect(host="localhost", database="coursework_5", user="postgres", password="Smash2012")
        with conn.cursor() as cur:
            # Создание таблиц employers и vacancies
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS employers (
                        employer_id SERIAL PRIMARY KEY,
                        company_name VARCHAR(255),
                        open_vacancies INTEGER
                        )""")

            cur.execute("""
                        CREATE TABLE IF NOT EXISTS vacancies (
                        vacancy_id SERIAL PRIMARY KEY,
                        vacancies_name VARCHAR(255),
                        payment INTEGER,
                        requirement TEXT,
                        vacancies_url TEXT,
                        employer_id INTEGER REFERENCES employers(employer_id)
                        )""")
            # Теперь коммитим изменения в базу данных
            conn.commit()
    except psycopg2.Error as e:
        print(f"Произошла ошибка при добавлении данных в таблицы: {e}")


def add_to_table(employers_list):
    """Заполнение базы данных компании и вакансии"""
    try:
        # Создание таблиц, если они еще не существуют
        create_table()

        # Подключение к базе данных coursework_5
        conn = psycopg2.connect(host="localhost", database="coursework_5", user="postgres", password="Smash2012")
        with conn.cursor() as cur:
            for employer in employers_list:
                employer_data = get_employer(employer)
                if employer_data:
                    cur.execute('INSERT INTO employers (employer_id, company_name, open_vacancies) '
                                'VALUES (%s, %s, %s) RETURNING employer_id',
                                (employer_data['employer_id'], employer_data['company_name'],
                                 employer_data['open_vacancies']))

            for employer in employers_list:
                vacancy_list = get_vacancies(employer)
                for v in vacancy_list:
                    cur.execute(
                        'INSERT INTO vacancies (vacancies_name, payment, requirement, vacancies_url, employer_id) '
                        'VALUES (%s, %s, %s, %s, %s)',
                        (v['vacancies_name'], v['payment'], v['requirement'], v['vacancies_url'], v['employer_id']))
        conn.commit()
        print("Данные успешно добавлены в таблицы.")
    except psycopg2.Error as e:
        print(f"Произошла ошибка при добавлении данных в таблицы: {e}")
