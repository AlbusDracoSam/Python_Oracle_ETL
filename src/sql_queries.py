import os
import pandas as pd

class SQLQueries:

    @staticmethod
    def create_table_query():
        query = """
        CREATE TABLE If not exists employees (
             emp_id INTEGER PRIMARY KEY,
             emp_name VARCHAR2 (100) NOT NULL,
             salary NUMBER(10, 2) NOT NULL,
             dept_id INTEGER NOT NULL,
             age INTEGER NOT NULL)"""
        return query

    @staticmethod
    def fetch_employees_query():
        query = """select * from employees"""
        return query

    @staticmethod
    def insert_employee_query():

        query = f"""
        INSERT INTO Employees (emp_id, emp_name, salary, dept_id, age)
        VALUES (:emp_id, :emp_name, :salary, :dept_id, :age)
        """
        return query





