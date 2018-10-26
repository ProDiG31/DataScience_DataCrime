import os
import mysql.connector

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
	print ("[INFO] - Veuillez pressez ENTER pour valider la valeur par default ")

	defaultUser = str(os.getenv("defaultUser"))
	defaultHost = str(os.getenv("defaultHost"))
	defaultPassword = str(os.getenv("defaultPassword"))
	databaseName = str(os.getenv("databaseName"))
    portName = str(os.getenv("databasePort"))

	inputHost 		= input("[INFO] - Veuillez saisir l' host de la base de donnée (default = "+ defaultHost +") : ")
	inputUser 		= input("[INFO] - Veuillez saisir l' identifiant de connexion à la base de donnée (default = "+ defaultUser +") : ")
	inputPassword 	= input("[INFO] - Veuillez saisir le password de connexion à la base de donnée (default = "+ defaultPassword +") : ")

	if len(inputHost) == 0 : 		inputHost = defaultHost
	if len(inputUser) == 0 : 		inputUser = defaultUser
	if len(inputPassword) == 0 : 	inputPassword = defaultPassword

	return mysql.connector.connect(host=inputHost,user=inputUser,password=inputPassword, database=databaseName,port=portName)

# 	conn = establishConnection()

# def getCursor():
# 	return conn.cursor()

# cursor = getCursor()

# def closeConnection():
# 	print ("[INFO] - Close database connection ")
# 	conn.close()

# def closeCursor():
# 	print ("[INFO] - Close database cursor ")
# 	cursor.close()