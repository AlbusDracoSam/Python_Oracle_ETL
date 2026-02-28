import oracledb
import os
import json
from sql_queries import SQLQueries

sql_queries = SQLQueries()

class DBService:
    def __init__(self):
        self.conn = None

    def fetch_creds(self):
        file_dir = os.path.dirname(__file__)
        file_path = os.path.join(file_dir, "db_config.json")
        try:
            with open(file_path) as f:
                db_config = json.load(f)
        except Exception as e:
            print(f"Error while reading file {file_path}")
            raise e

        print("Fetching credentials")
        return db_config

    def connect(self):
        if self.conn is None:
            try:
                db_config = self.fetch_creds()
                self.conn = oracledb.connect(**db_config)
                with self.conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM dual")
                    print(f"Connected to DB")
            except oracledb.Error as e:
                print(f"Connection has failed. {e}")
                raise e

    def exec_query(self, query, params = None):
        if self.conn is None:
            self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params or {})
            if cursor.description is not None:
                return cursor.fetchall()
            self.conn.commit()
            return cursor.rowcount
        except oracledb.Error as e:
            print(f"Connection has failed. {e}")
            raise e

    def disconnect(self):
        if self.conn:
            try:
                self.conn.close()
                print(f"Disconnected from DB")
            except oracledb.Error as e:
                print(f"Disconnection has failed. {e}")
            finally:
                self.conn = None


