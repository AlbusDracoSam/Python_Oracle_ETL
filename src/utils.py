import os
import pandas as pd
from src.sql_queries import SQLQueries
from src.db_service import DBService

sql_queries = SQLQueries()
db = DBService()

EXPECTED_SCHEMA ={
    "emp_id": int,
    "emp_name": str,
    "salary": float,
    "dept_id": int,
    "age": int,
}

NOT_NULL_COLUMNS = {"emp_id", "emp_name", "salary", "dept_id", "age"}

class Utils:

    def read_employees_csv(self):
        try:
            file_dir = os.path.dirname(__file__)
            file_path = os.path.join(file_dir, "resources/Employees_table.csv")
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            print("Error while reading employees csv")
            raise e

    def insert_employee_row_by_row(self, df, query):
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

    def insert_employee_batch(self, conn, df, query):
        try:
            df = self.validate_schema(df)
            inserted = db.exec_batch(conn, query, df)
            expected = len(df)
            if inserted != expected:
                raise RuntimeError(f"Partial data inserted. Expected={expected}, inserted={inserted}")
            print(f"Successfully inserted {inserted} employees")
        except Exception as e:
            print("Error while inserting employee")
            raise e

    def insert_employee_batch_with_quarantine(self, conn, df, query):
        try:
            inserted = db.exec_batch_with_quarantine(conn, df, query)
            expected = len(df)
            if inserted != expected:
                raise RuntimeError(f"Partial data inserted. Expected={expected}, inserted={inserted}")
            print(f"Successfully inserted {inserted} employees")
        except Exception as e:
            print("Error while inserting employee")
            raise e

    def validate_schema(self, df):

        try:
        # 1. Check missing columns
            missing = set(EXPECTED_SCHEMA.keys()) - set(df.columns)
            if missing:
                raise RuntimeError(f"Missing Columns: {missing}")

            # 2. Check nulls
            nulls = [c for c in NOT_NULL_COLUMNS if df[c].isnull().any()]
            if nulls:
                raise ValueError(f"Null values found in NOT NULL columns : {nulls}")

            # 3. Type validation
            for col, expected_type in EXPECTED_SCHEMA.items():
                try:
                    df[col] = df[col].astype(expected_type)
                except Exception as e:
                    raise TypeError(f"Error while converting {col} to type {expected_type}")

            # 4. Business rules
            if (df['age'] < 18).any():
                raise ValueError(f"Age must be less than or equal to 18")

            print("Successfully validated schema")
            return df
        except Exception as e:
            print("Error while validating schema")
            raise e



