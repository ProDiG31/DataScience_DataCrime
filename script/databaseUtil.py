import os
import mysql.connector
import time

# -------- VARIABLES DECLARATION ZONE  ------------------

# table_import_temp = os.getenv("t_crime_import")

class Database:
    def __init__(self):
        self._conn = establishConnection()
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()


def establishConnection():
    print ("[INFO] - procedure de connexion à la base de donnee MYSQL  ")
    time.sleep(5)
    return mysql.connector.connect(
        host=os.getenv('defaultHost'),
        user=os.getenv('defaultUser'),
        password=os.getenv('defaultPassword'),
        database=os.getenv('databaseName'),
        port=os.getenv('databasePort'),
        auth_plugin='mysql_native_password'
    )
