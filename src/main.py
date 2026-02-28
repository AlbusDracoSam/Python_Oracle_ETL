from src.db_service import DBService
from sql_queries import SQLQueries
from utils import Utils

def main():
    db = DBService()
    sql_queries = SQLQueries()
    utils = Utils()
    try:
        db.connect()
        create_query = sql_queries.create_table_query()
        result = db.exec_query(create_query)
        df = utils.read_employees_csv()
        insert_query = sql_queries.insert_employee_query()
        utils.insert_employee(df, insert_query)

    except Exception as e:
        print(e)
    finally:
        db.disconnect()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
