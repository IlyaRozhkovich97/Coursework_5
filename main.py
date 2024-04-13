import psycopg2
from utils import create_table, add_to_table
from db_manager import DBManager
import time


def main():
    employers_list = [1740, 561525, 8620, 8642172, 3529, 78638, 4006, 4504679, 15478, 64174]

    db_manager = DBManager(dbname="coursework_5", user="postgres", password="Smash2012")

    # Попытка создания таблиц, ожидание, если база данных занята другими пользователями
    for _ in range(5):
        try:
            create_table()
            print("Таблицы успешно созданы!")
            break
        except psycopg2.errors.ObjectInUse as e:
            print(f"Не удалось создать таблицы: {e}")
            print("Ждем 5 секунд перед повторной попыткой...")
            time.sleep(5)
    else:
        print("Не удалось создать таблицы: база данных занята другими пользователями.")
        return

    # Добавление данных в таблицы
    add_to_table(employers_list)

    while True:
        print(
            "1. Получить список всех компаний и количество вакансий у каждой компании\n"
            "2. Получить список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на "
            "вакансию\n"
            "3. Получить среднюю зарплату по вакансиям\n"
            "4. Получить список всех вакансий, у которых зарплата выше средней по всем вакансиям\n"
            "5. Получить список всех вакансий, в названии которых содержатся переданные в метод слова\n"
            "6. Завершить работу"
        )

        task = input("Введите номер: ")

        if task == "6":
            break
        elif task == '1':
            print(db_manager.get_companies_and_vacancies_count())
            print()
        elif task == '2':
            print(db_manager.get_all_vacancies())
            print()
        elif task == '3':
            print(db_manager.get_avg_salary())
            print()
        elif task == '4':
            print(db_manager.get_vacancies_with_higher_salary())
            print()
        elif task == '5':
            keyword = input('Введите ключевое слово: ')
            print(db_manager.get_vacancies_with_keyword(keyword))
            print()
        else:
            print('Неправильный запрос')

        continue_input = input("Хотите продолжить? (Да/Нет): ")
        if continue_input.lower() != 'да':
            break


if __name__ == "__main__":
    main()
