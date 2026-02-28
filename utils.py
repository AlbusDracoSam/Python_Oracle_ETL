import os
import pandas as pd
from sql_queries import SQLQueries
from db_service import DBService

sql_queries = SQLQueries()
db = DBService()

class Utils:

    def read_employees_csv(self):
        try:
            file_dir = os.path.dirname(__file__)
            file_path = os.path.join(file_dir, "Employees_table.csv")
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            print("Error while reading employees csv")
            raise e

    def insert_employee(self, df, query):
        try:
            for _, row in df.iterrows():
                print(query)
                db.exec_query(query, {"emp_id": int(row.emp_id), "emp_name": row.emp_name, "salary": float(row.salary),
                                        "dept_id": int(row.dept_id), "age": int(row.age)})
                print(f"Employee {row.emp_id} inserted")
            return ""
        except Exception as e:
            print("Error while inserting employee")
            raise e