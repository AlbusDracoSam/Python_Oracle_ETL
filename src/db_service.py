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
        file_path = os.path.join(file_dir, "resources/db_config.json")
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
                return self.conn
            except oracledb.Error as e:
                print(f"Connection has failed. {e}")
                raise e
        return None

    def exec_query(self, conn, query, params = None):
        if conn is None:
            self.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params or {})
            if cursor.description is not None:
                return cursor.fetchall()
            conn.commit()
            return cursor.rowcount
        except oracledb.Error as e:
            print(f"Connection has failed. {e}")
            raise e

    def disconnect(self, conn):
        if conn:
            try:
                conn.close()
                print(f"Disconnected from DB")
            except oracledb.Error as e:
                print(f"Disconnection has failed. {e}")
            finally:
                conn = None

    def exec_batch(self, conn, query, df):
        if conn is None:
            conn = self.connect()
        try:
            cursor = conn.cursor()
            data = df.to_dict(orient="records")
            cursor.executemany(query, data)
            conn.commit()
            print("Batch inserted into DB")
            return cursor.rowcount
        except oracledb.Error as e:
            print(f"Connection has failed. {e}")
            raise e


