# coding: utf-8
import mysql.connector
import csv
import os
import time
import datetime

# importing database class
from databaseUtil import Database

# -------- VARIABLES DECLARATION ZONE  ------------------

databaseName = os.getenv("databaseName")
table_import_temp = os.getenv("table_import_temp")

verbose = os.getenv("verbose") == "True"
debug = os.getenv("debug") == "True"

# -------- FUNCTIONS WHICH BE EXECUTED BELOW ------------------

def initializeDatabase(cursor):
	print("[INFO] - Creating Database")
	cursor.execute("CREATE DATABASE IF NOT EXISTS %s ;" % databaseName)

def useDb(cursor):
	print("[INFO] - db_dataCrime database Selected")
	cursor.execute("USE %s;" % databaseName )

def clearDb(cursor):
	print ("[INFO] - Dropping Previous Tables")
	cursor.execute("DROP TABLE IF EXISTS crime;")
	cursor.execute("DROP TABLE IF EXISTS area;")
	cursor.execute("DROP TABLE IF EXISTS weapon_type;")
	cursor.execute("DROP TABLE IF EXISTS crime_type;")
	cursor.execute("DROP TABLE IF EXISTS premise_type;")
	cursor.execute("DROP TABLE IF EXISTS modus_operandi_type;")
	cursor.execute("DROP TABLE IF EXISTS status_type;")
	cursor.execute("DROP TABLE IF EXISTS descent_type;")
	cursor.execute("DROP TABLE IF EXISTS fullmoon;")
	cursor.execute("DROP TABLE IF EXISTS demographie_LA;")
	cursor.execute("DROP TABLE IF EXISTS unemployement_LA;")
	cursor.execute("DROP TABLE IF EXISTS single_parent_rate;")
	cursor.execute("DROP TABLE IF EXISTS police_budgets;")


def clearTable(cursor):
	print ("[INFO] - Dropping Previous Tables")
	cursor.execute("DROP TABLE IF EXISTS %s;" % table_import_temp)

def configDB(cursor): 
	print ("[INFO] - Configuring database sql_mode")
	cursor.execute("SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';")

def initializeTableImport(cursor):
	print ("[INFO] - Creating TABLE %s" % table_import_temp)
	cursor.execute("""
	CREATE TABLE IF NOT EXISTS t_crime_import (
		DR_Number varchar(9),
		date_reported DATE,
		date_occurred DATE,
		time_occurred varchar(4),
		id_area  int(2),
		area_name varchar(100),
		reporting_district int(4),
		crime_code int(3),
		crime_code_description varchar(100),
		MO_codes text,
		victim_age int(3),
		victim_sex varchar(1),
		victim_descent varchar(1),
		premise_code int(3),
		premise_description varchar(100),
		weapon_used_code int(3),
		weapon_description varchar(100),
		status_code varchar(5),
		status_description varchar(100),
		crime_code_1 varchar(100),
		crime_code_2 varchar(100),
		crime_code_3 varchar(100),
		crime_code_4 varchar(100),
		address varchar(100),
		cross_street varchar(100),
		location varchar(100),
		constraint pk_id_DR_Number PRIMARY KEY (DR_Number)
	) ENGINE=InnoDB ;
	""")

def ImportRowInTempTable(query,row):
	if (verbose) :print ("[INFO] - Inserting Or Updating Raw into TABLE t_crime_import : "+ row["DR Number"])
	if (debug): print(row)

	victim_age = 0
	premise_Code = 0
	weapon_Used_code = 0

	if (row['Victim Age'] != '') : victim_age = int(row['Victim Age'])
	if (row['Weapon Used Code'] != '') : weapon_Used_code = int(row['Weapon Used Code'])
	if (row['Premise Code'] != '') : premise_Code = int(row['Premise Code'])

	dateFormat = '%m/%d/%Y'
	dateReport = "STR_TO_DATE('%s','%s')" % (row["Date Reported"], dateFormat)
	dateOccur = "STR_TO_DATE('%s','%s')" % (row["Date Occurred"], dateFormat)

	# print (dateReport)
	# input ('he')
	query += ("""(%s, %s, %s, %s, %s,'%s', %s, %s, "%s", "%s",%s, "%s", "%s", %s, "%s", %s, "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s"),""" % (
		row["DR Number"],
		dateReport,
		dateOccur,
		row["Time Occurred"],
		int(row["Area ID"]),
		row['Area Name'],
		int(row['Reporting District']),
		int(row['Crime Code']),
		row['Crime Code Description'],
		row['MO Codes'],
		victim_age,
		row['Victim Sex'],
		row['Victim Descent'],
		premise_Code,
		row['Premise Description'],
		weapon_Used_code,
		row['Weapon Description'],
		row['Status Code'],
		row['Status Description'],
		row['Crime Code 1'],
		row['Crime Code 2'],
		row['Crime Code 3'],
		row['Crime Code 4'],
		row['Address'],
		row['Cross Street'],
		row['Location']
		)
	)
	return query

def importDemographieLA(cursor,row):
	if (verbose) : print ("[INFO] - Inserting import data into TABLE demographie_LA : "+ row["Year"])
	if (debug): print(row)
	query = "INSERT INTO demographie_LA (year, population) VALUES (%s, %s) " % (row["Year"],row['Population'])
	if (debug):print("[INFO] - " + query)
	cursor.execute(query)

def importFullMoonData(cursor,row):
	if (verbose) : print ("[INFO] - Inserting import data into TABLE fullmoon : "+ row["full_moon_date"])
	if (debug): print(row)
	date = str(row["full_moon_date"])
	query = "INSERT INTO fullmoon (date_fullmoon) VALUES (STR_TO_DATE(\'" + date + "\', '%m/%d/%Y'))"
	if (debug): print("[INFO] - " + query)
	cursor.execute(query)

def importPoliceBudgetPerYear(cursor, row):
	if (verbose) : print ("[INFO] - Importing PoliceBudget Value ")
	if (debug) : print (row)
	year = row['year']
	budget = row['budget']
	query = "INSERT INTO police_budgets (year_budget, police_budget) VALUES (\"%s\", \"%s\")" % (year, budget)
	if (debug) : print (query)
	cursor.execute(query)

def importUnemployementRate(cursor,row):
	if (verbose) : print ("[INFO] - Inserting import data into TABLE unemployement_LA : "+ row["date"])
	if (debug): print(row)

	datepatern = '%Y-%m-%d'
	query = "INSERT INTO unemployement_LA (date,unemployement_rate) VALUES (STR_TO_DATE('%s', '%s'),%s)" % (row['date'], datepatern, float(row['unemployement_rate']))
	if (debug): print("[INFO] - " + query)
	cursor.execute(query)

def importSingleParentRate(cursor,row):
	if (verbose) : print ("[INFO] - Inserting import data into TABLE single_parent_rate : "+ row["date"])
	if (debug): print(row)
	datepatern = '%Y-%m-%d'
	query = "INSERT INTO single_parent_rate (date,single_parent_rate) VALUES (STR_TO_DATE('%s', '%s'),%s)" % (row['date'], datepatern, float(row['single_parent_rate']))
	if (debug): print("[INFO] - " + query)
	cursor.execute(query)

def initializeFullMoonTable(cursor):
	if (verbose) : print ("[INFO] - Creating TABLE fullmoon")
	cursor.execute("""
	CREATE TABLE IF NOT EXISTS fullmoon (
		id_fullmoon int(2) NOT NULL AUTO_INCREMENT,
		date_fullmoon DATE,
		CONSTRAINT pk_id_fullmoon PRIMARY KEY(id_fullmoon)
	) ENGINE=InnoDB;
	""")

def initializeDemographieLA(cursor):
	if (verbose) : print ("[INFO] - Creating TABLE demographie_LA")
	cursor.execute("""
	CREATE TABLE IF NOT EXISTS demographie_LA (
		year varchar(4),
		population int(10)
	) ENGINE=InnoDB;
	""")

def initializeUnemployementLA(cursor):
	if (verbose) : print ("[INFO] - Creating TABLE unemployement_LA")
	cursor.execute("""
	CREATE TABLE IF NOT EXISTS unemployement_LA (
		date DATE,
		unemployement_rate DECIMAL(4,2)
	) ENGINE=InnoDB;
	""")

def initializeSingleParentRate(cursor):
	if (verbose) : print ("[INFO] - Creating TABLE single_parent_rate")
	cursor.execute("""
	CREATE TABLE IF NOT EXISTS single_parent_rate (
		date DATE,
		single_parent_rate DECIMAL(4,2)
	) ENGINE=InnoDB ;
	""")

def initializeNormalizedTable(cursor):
	if (verbose) : print ("[INFO] - Creating TABLE area ")
	cursor.execute("""
	       CREATE TABLE area (
		id_area int(2) NOT NULL,
		area_name varchar(100) NOT NULL,
		CONSTRAINT pk_id_Area PRIMARY KEY(id_area)
	) ENGINE=InnoDB;
	""")

	if (verbose) : print ("[INFO] - Creating TABLE weapon_type")
	cursor.execute("""
		CREATE TABLE weapon_type (
			id_weapon int(3) NOT NULL,
			weapon_description varchar(100) NOT NULL,
			CONSTRAINT pk_id_Weapon PRIMARY KEY(id_weapon)
		) ENGINE=InnoDB;
	""")

	if (verbose) : print ("[INFO] - Creating TABLE crime_type ")
	cursor.execute("""
		CREATE TABLE crime_type (
			id_crime int(3) NOT NULL,
			crime_code_description varchar(100) NOT NULL,
			category varchar(100) NOT NULL,
			gravity varchar(100) NOT NULL,
			CONSTRAINT pk_id_Crime PRIMARY KEY(id_crime)
		) ENGINE=InnoDB;
	""")

	if (verbose) : print ("[INFO] - Creating TABLE premise_type")
	cursor.execute("""
		CREATE TABLE premise_type (
			id_premise int(3) NOT NULL,
			premise_description varchar(100) NOT NULL,
			CONSTRAINT pk_id_Premise PRIMARY KEY(id_premise)
		) ENGINE=InnoDB;
	""")

	if (verbose) : print ("[INFO] - Creating TABLE modus_operandi_type ")
	cursor.execute("""
		CREATE TABLE modus_operandi_type (
			id_modus_operandi int(3) NOT NULL,
			modus_operandi_description varchar(200) NOT NULL,
			CONSTRAINT pk_id_Modus_Operandi PRIMARY KEY(id_modus_operandi)
		) ENGINE=InnoDB;
	""")

	if (verbose) : print ("[INFO] - Creating TABLE status_type ")
	cursor.execute("""
		CREATE TABLE status_type (
			id_status varchar(5) NOT NULL,
			status_description varchar(100) NOT NULL,
			CONSTRAINT pk_id_Status PRIMARY KEY(id_status)
		) ENGINE=InnoDB;
	""")

	if (verbose) : print ("[INFO] - Creating TABLE descent_typeDescend_type ")
	cursor.execute("""
		CREATE TABLE descent_type (
			id_descent varchar(1) NOT NULL,
			descent_description varchar(100) NOT NULL,
			CONSTRAINT pk_id_Descent PRIMARY KEY(id_descent)
		) ENGINE=InnoDB;
	""")

	if (verbose) : print ("[INFO] - Creating TABLE crime")
	cursor.execute("""
		CREATE TABLE crime (
			id_crime varchar(9),
			date_reported DATE,
			date_occurred DATE,
			time_occurred varchar(4),
			id_area  int(2),
			reporting_district int(4),
			crime_code int(3),
			MO_codes varchar(200),
			victim_age int(3),
			victim_sex varchar(1),
			victim_descent varchar(1),
			premise_code int(3),
			weapon_used_code int(3),
			status_code varchar(5),
			crime_code_1 varchar(100),
			crime_code_2 varchar(100),
			crime_code_3 varchar(100),
			crime_code_4 varchar(100),
			address varchar(100),
			cross_street varchar(100),
			location varchar(100),
			CONSTRAINT pk_id_crime PRIMARY KEY (id_crime)
		) ENGINE=InnoDB;
	""")

	cursor.execute("""
		CREATE TABLE police_budgets (
			id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
			year_budget SMALLINT UNSIGNED,
			police_budget INT(12) UNSIGNED,
			CONSTRAINT pk_id_police_budget PRIMARY KEY (id)
		)
	""")

#			CONSTRAINT fk_id_Area FOREIGN KEY (Area_ID) REFERENCES area(id_area),
#			CONSTRAINT fk_Crime_Code FOREIGN KEY (Crime_Code) REFERENCES crime_type(id_Crime),
#			CONSTRAINT fk_MO_Codes FOREIGN KEY (MO_Codes) REFERENCES modus_operandi_type(id_Modus_operandi),
#			CONSTRAINT fk_Premise_Code FOREIGN KEY (Premise_Code) REFERENCES premise_type(id_Premise),
#			CONSTRAINT fk_Weapon_Used_Code FOREIGN KEY (Weapon_Used_Code) REFERENCES weapon_type(id_Weapon),
#			CONSTRAINT fk_Status_Code FOREIGN KEY (Status_Code) REFERENCES status_type(id_Status),
#			CONSTRAINT fk_Descend_type FOREIGN KEY (Victim_Descent) REFERENCES descent_type(id_Descent)

def importMoCodes(cursor,row):
	if (verbose) : print ("[INFO] - Importing Mo_Codes : "+ row['id_Modus_Operandi'])
	if (debug) : print (row)
	id_Mo = row['id_Modus_Operandi']
	Desc_Mo = row['modus_Operandi_Description']
	query = "INSERT INTO modus_operandi_type (id_modus_operandi,modus_operandi_description )  VALUES (%s, \"%s\")" % (id_Mo, Desc_Mo)
	if (debug) : print (query)
	cursor.execute(query)

def importDescent(cursor,row):
	if (verbose) : print ("[INFO] - Importing Descent Value : "+ row['Descent_Description'])
	if (debug) : print (row)
	id_Desc = row['Descent_Code']
	desc_Desc = row['Descent_Description']
	query = "INSERT INTO descent_type (id_descent,descent_description) VALUES (\"%s\", \"%s\")" % (id_Desc, desc_Desc)
	if (debug) : print (query)
	cursor.execute(query)

# def importWeapon(cursor,row):
# 	if (verbose) : print ("[INFO] - Importing Weapons : "+ row['weapons_code'])
# 	if (debug) : print (row)

# 	query = """INSERT INTO weapon_type(id_weapon,weapon_description,category,gravity)
# 				VALUES (%s, '%s', '%s', '%s')""" % (
# 							row['weapons_code'],
# 							row['weapon_description'],
# 							row['Category'],
# 							row['gravity']
# 							)

# 	if (debug) : print (query)
# 	cursor.execute(query)

def extractWeapon(cursor):
	if (verbose) : print ("[INFO] - Extracting Weapons from t_crime ")
	cursor.execute("""
			  INSERT INTO weapon_type(id_weapon,weapon_description)
			  SELECT Distinct weapon_used_code, weapon_description
				  FROM t_crime_import;""")

def extractPremise(cursor):
	if (verbose) : print ("[INFO] - Extracting Premise from t_crime ")
	cursor.execute("""
			  INSERT INTO premise_type(id_premise,premise_description) ON DUPLICATE KEY UPDATE
			  SELECT Distinct premise_code,premise_description
				FROM t_crime_import
				where premise_description != ''
				group by premise_code
				order by premise_code;
			""")

def extractArea(cursor) :
	if (verbose) : print ("[INFO] - Extracting Area from t_crime ")
	cursor.execute("""
			  INSERT INTO area(id_area,area_name)
			  SELECT Distinct id_area,area_name
				  FROM t_crime_import;
			""")

def importCrimeType(cursor,row):
	if (verbose) : print ("[INFO] - Importing Weapons : "+ row['id_crime'])
	if (debug) : print (row)
	query = """INSERT INTO crime_type(id_crime,crime_code_description,category,gravity)
				VALUES (%s, '%s', '%s', '%s')""" % (
							row['id_crime'],
							row['crime_code_description'],
							row['category'],
							row['gravity']
							)

	if (debug) : print (query)
	cursor.execute(query)

def importCrimeFact(cursor) :
	if (verbose) : print ("[INFO] - Normalise declaration from t_crime_import ")
	cursor.execute("""
			  INSERT INTO crime (
							id_crime,
							date_reported,
							date_occurred,
							time_occurred,
							id_area,
							reporting_district,
							crime_code,
							MO_codes,
							victim_age,
							victim_sex,
							victim_descent,
							premise_code,
							weapon_used_code,
							status_code,
							crime_code_1,
							crime_code_2,
							crime_code_3,
							crime_code_4,
							address,
							cross_street,
							location
							)
			  SELECT
					DR_Number,
					date_reported,
					date_occurred,
					time_occurred,
					id_area,
					reporting_district,
					crime_code,
					MO_codes,
					victim_age,
					victim_sex,
					victim_descent,
					premise_code,
					weapon_used_code,
					status_code,
					crime_code_1,
					crime_code_2,
					crime_code_3,
					crime_code_4,
					address,
					cross_street,
					location
		          FROM t_crime_import;
		""")

def importTempTable(cursor):
	# ---- Read Cleared data imported from US Open Data
	# filename = '../source/CrimeDataLight.csv'
	# filename = '../source/CrimeDataLight2.csv'
	filename = '../source/Crime_Data_from_2010_to_Present.csv'
	print ('[INFO] - Counting Row ')
	totalrows =  len(open(filename).readlines()) - 1
	t0 = time.time()

	with open(filename) as csvfile:
	# ---- Read each line of csv file
		reader = csv.DictReader(csvfile, delimiter=',' , lineterminator ='\n')
		rowNumber = 0
		print ('[INFO] - line %d: %s' % (rowNumber, totalrows))
		insert = ("""
			INSERT IGNORE INTO t_crime_import (
				DR_Number,
				date_reported,
				date_occurred,
				time_occurred,
				id_area,
				area_name,
				reporting_district,
				crime_code,
				crime_code_description,
				MO_codes,
				victim_age,
				victim_sex,
				victim_descent,
				premise_code,
				premise_description,
				weapon_used_code,
				weapon_description,
				status_code,
				status_description,
				crime_code_1,
				crime_code_2,
				crime_code_3,
				crime_code_4,
				address,
				cross_street,
				location
				)
			VALUES
		""")

		query = insert
		for row in reader:
			# ---- Import each row into Mysql Database
			query = ImportRowInTempTable(query,row)

			# ---- Log current activities
			rowNumber += 1
			percent = rowNumber * 100 / totalrows
			elapsedTime = time.time() - t0
			try:
				rate = rowNumber // elapsedTime
			except ZeroDivisionError:
				rate = 1
			timeLeftSec = abs((rowNumber - totalrows) / rate)
			timeLeftString = str(datetime.timedelta(seconds=timeLeftSec))
			print ('[INFO] - line %d : %s	- Progressing :	%.3f%%	- Elapsed time : %.2f sec (%s) - Rate : %.2f input/Sec' %
							(rowNumber,
							 totalrows,
							 percent,
							 elapsedTime,
							 timeLeftString,
							 rate
							 ))
			if ((rowNumber % 20 == 0) or (rowNumber == totalrows)):
				# print (rowNumber)
				query = query[:-1] # Replacing last ','
				query += ";"
				# print (query)
				cursor.execute(query)
				query = insert

	print('[INFO] - Elapsed time : %.2f sec' % (time.time() - t0))

# def defineDatabase(Database database):

def deployDatabase(database):
	# -------- SCRIPT BEGINS HERE AND WILL EXECUTE FUNCTIONS DECLARED ABOVE
	# ---- Connection to Mysql Server
	conn = database

	# ---- Init Mysql Cursor
	cursor = conn.cursor

	# ---- Creating database
	initializeDatabase(cursor)
	conn.commit()

	# ---- Select Database
	useDb(cursor)

	# ---- Clear Database
	clearDb(cursor)
	conn.commit()

	# ---- Ask to import temp data
	isDeployed = input("[INFO] - Souhaitez vous importer les données temporaire dans la base de donnée (Y/N) :")
	if(isDeployed == "Y"):

		# ---- Drop Temp Table
		clearTable(cursor)

		# ---- Create Temp Table for inline Import
		initializeTableImport(cursor)

		# ----- Configuring SQL_MODE
		configDB(cursor)

		# ---- commit Changes into DataBase
		conn.commit()

		importTempTable(cursor)
		t0 = time.time()
		print('[INFO] - Commiting request into database')
		conn.commit()
		print('[INFO] - Elapsed time : %.2f sec' % (time.time() - t0))

	else: print("[INFO] - Import skipped")

	# ---- create normalized tables
	initializeNormalizedTable(cursor)

	importCrimeFact(cursor)
	conn.commit()

	# ---- Infill new Table from t_crime_import
	extractArea(cursor)
	extractWeapon(cursor)
	extractPremise(cursor)
	# extractCrimeType(cursor)
	conn.commit()

	# ---- fill Crime code type data from Crime_code_gravity.csv
	csvLink = '../source/Crime_code_gravity.csv'
	with open(csvLink) as csvLinkRead :
		reader = csv.DictReader(csvLinkRead, delimiter = ',', lineterminator = '\r')
		for row in reader:
			importCrimeType(cursor,row)
	conn.commit()

	# ---- fill Mo Codes data from mo_code.csv
	csvLink = '../source/mo_code.csv'
	with open(csvLink) as csvLinkRead :
		reader = csv.DictReader(csvLinkRead, delimiter = ';', lineterminator = '\r')
		for row in reader:
			importMoCodes(cursor,row)
	conn.commit()


	# ---- fill Descent Value data from descent_code.csv
	csvLink = '../source/descent_code.csv'
	with open(csvLink) as csvLinkRead :
		reader = csv.DictReader(csvLinkRead, delimiter = ',')
		for row in reader:
			importDescent(cursor,row)
	conn.commit()

	# ---- create normalized tables
	initializeFullMoonTable(cursor)
	initializeDemographieLA(cursor)
	initializeUnemployementLA(cursor)
	initializeSingleParentRate(cursor)

	# ---- fill Full Moon data from full_moon.csv
	csvLink = '../source/full_moon.csv'
	with open(csvLink) as csvLinkRead :
		reader = csv.DictReader(csvLinkRead, delimiter = ',')
		for row in reader:
			importFullMoonData(cursor,row)
	conn.commit()

	# ---- fill Demogrphic data from demo_LA.csv
	csvLink = '../source/demo_LA.csv'
	with open(csvLink) as csvLinkRead :
		reader = csv.DictReader(csvLinkRead, delimiter = ',')
		for row in reader:
			importDemographieLA(cursor,row)
	conn.commit()

	# ---- fill Unemployement Rate data from Unemployement_Rate_LA.csv
	csvLink = '../source/Unemployement_Rate_LA.csv'
	with open(csvLink) as csvLinkRead :
		reader = csv.DictReader(csvLinkRead, delimiter = ',')
		for row in reader:
			importUnemployementRate(cursor,row)
	conn.commit()

	# ---- fill Single Parent Rate data from demo_LA.csv
	csvLink = '../source/SingleParentRate.csv'
	with open(csvLink) as csvLinkRead :
		reader = csv.DictReader(csvLinkRead, delimiter = ',')
		for row in reader:
			importSingleParentRate(cursor,row)
	conn.commit()

	csvLink = '../source/police_budget.csv'
	with open(csvLink) as csvLinkRead :
		reader = csv.DictReader(csvLinkRead, delimiter = ',')
		for row in reader:
			importPoliceBudgetPerYear(cursor, row)
	conn.commit()