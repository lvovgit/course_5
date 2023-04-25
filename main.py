from hh_parser import HeadHunter
from db_manager import DBManager
from config import config
PATH = 'employers.json'

def main():
    params = config()
    db = DBManager('head_hunter', params)


    print('Создаем базу данныч и таблицы')
    db.create_database()
    print(f'База данных и таблицы созданы')
    #employers_from_json = read_json(PATH)





# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
