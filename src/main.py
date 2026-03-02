from src.db_service import DBService
from sql_queries import SQLQueries
from utils import Utils

def main():
    conn = None
    db = DBService()
    sql_queries = SQLQueries()
    utils = Utils()
    try:
        conn = db.connect()
        create_table_query = sql_queries.create_table_query()
        result = db.exec_query(conn, create_table_query)
        create_quarantine_query = sql_queries.create_quarantined_table_query()
        result = db.exec_query(conn, create_quarantine_query)
        result = db.exec_query(conn, create_table_query)
        df = utils.read_employees_csv()
        insert_query = sql_queries.insert_employee_query()
        utils.insert_employee_row_by_row(df, insert_query) #Insert data row by row
        utils.insert_employee_batch(conn, df, insert_query) #Insert in batch (whole)
        utils.insert_employee_batch_with_quarantine(conn, df, insert_query) # Insert with quarantine



    except Exception as e:
        print(e)
    finally:
        db.disconnect(conn)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
