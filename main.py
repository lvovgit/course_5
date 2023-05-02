from db_manager import DBManager
from config import config
from utils import read_json
from hh_parser import HeadHunter

PATH = 'vacancies.json'
PATH_2 = 'employers.json'

def main():
    params = config()
    db = DBManager('head_hunter', params)
    print('Создаем базу данных и таблицы')
    db2 = db.create_database()
    print(f'База данных и таблицы созданы')
    search_keyword = 'Python'
    hh = HeadHunter(search_keyword)
    k = hh.get_request()
    print('Добавляем данные')
    db.insert_data_into_db(k)
    # employers_from_json = read_json(PATH_2)
    # print('Добавляем данные о работодателях')
    #
    # db.insert('employers', employers_from_json)
    # print('Данные о работодателях добавлены')
    # print('Добавляем данные о вакансиях')
    # print('................................')
    # hh = HeadHunter.get_vacancies
    # vacancies_from_json = read_json(PATH)
    # for i in range(len(employers_from_json)):
    #     db.insert('vacancies', vacancies_from_json)
    # print('Данные о вакансиях добавлены')
    #

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
