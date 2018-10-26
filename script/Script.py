# coding: utf-8
import mysql.connector
import csv

# -------- VARIABLES DECLARATION ZONE  ------------------

defaultUser = "root"
defaultHost = "localhost"
defaultPassword = "root"

database = "db_dataCrime"
table_import_temp = "t_crime_import"

verbose = True
debug = False
# -------- FUNCTIONS WHICH BE EXECUTED BELOW ------------------

def initializeDatabase():
	print("[INFO] - Creating Database")
	cursor.execute("CREATE DATABASE IF NOT EXISTS %s ;" % database)

def useDb():
	print("[INFO] - db_dataCrime database Selected")
	cursor.execute("USE %s;" % database )

def clearDb():
	print ("[INFO] - Dropping Previous Tables")
	cursor.execute("DROP TABLE IF EXISTS %s;" % table_import_temp)
	cursor.execute("DROP TABLE IF EXISTS crime;")
	cursor.execute("DROP TABLE IF EXISTS area;")
	cursor.execute("DROP TABLE IF EXISTS weapon_type;")
	cursor.execute("DROP TABLE IF EXISTS crime_type;")
	cursor.execute("DROP TABLE IF EXISTS premise_type;")
	cursor.execute("DROP TABLE IF EXISTS modus_operandi_type;")
	cursor.execute("DROP TABLE IF EXISTS status_type;")
	cursor.execute("DROP TABLE IF EXISTS descent_type;")
	cursor.execute("DROP TABLE IF EXISTS fullMoon;")
	cursor.execute("DROP TABLE IF EXISTS demographie_LA;")
	cursor.execute("DROP TABLE IF EXISTS unemployement_LA;")
	cursor.execute("DROP TABLE IF EXISTS police_budgets;")

def initializeTableImport():
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

def ImportRowInTempTable(row):
	if (verbose) :print ("[INFO] - Inserting Or Updating Raw into TABLE t_crime_import : "+ row["DR Number"])
	if (debug): print(row)

	victim_age = 0
	if (row['Victim Age'] != '') : victim_age = int(row['Victim Age'])

	weapon_Used_code = 0
	if (row['Weapon Used Code'] != '') : weapon_Used_code = int(row['Weapon Used Code'])

	premise_Code = 0
	if(row['Premise Code'] != '') : premise_Code = int(row['Premise Code'])

	cursor.execute("""
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
	  VALUES (%s, STR_TO_DATE(%s, '%m/%d/%Y'), STR_TO_DATE(%s, '%m/%d/%Y'), %s, %s,
		    %s, %s, %s, %s, %s,
		    %s, %s, %s, %s, %s,
		    %s, %s, %s, %s, %s,
			%s, %s, %s, %s, %s,
			%s)
	""", (
		row["DR Number"],
		row["Date Reported"],
		row["Date Occurred"],
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

def importDemographieLA(row):
	if (verbose) : print ("[INFO] - Inserting import data into TABLE demographie_LA : "+ row["Year"])
	if (debug): print(row)
	query = "INSERT INTO demographie_LA (year, population) VALUES (%s, %s) " % (row["Year"],row['Population'])
	if (debug):print("[INFO] - " + query)
	cursor.execute(query)

def importFullMoonData(row):
	if (verbose) : print ("[INFO] - Inserting import data into TABLE fullmoon : "+ row["full_moon_date"])
	if (debug): print(row)
	date = str(row["full_moon_date"])
	query = "INSERT INTO fullmoon (date_fullmoon) VALUES (STR_TO_DATE(\'" + date + "\', '%m/%d/%Y'))"
	if (debug): print("[INFO] - " + query)
	cursor.execute(query)

def importUnemployementRate(row):
	if (verbose) : print ("[INFO] - Inserting import data into TABLE unemployement_LA : "+ row["date"])
	if (debug): print(row)

	datepatern = '%Y-%m-%d'
	query = "INSERT INTO unemployement_LA (date,unemployement_rate) VALUES (STR_TO_DATE('%s', '%s'),%s)" % (row['date'], datepatern, float(row['unemployement_rate']))
	if (debug): print("[INFO] - " + query)
	cursor.execute(query)

def importSingleParentRate(row):
	if (verbose) : print ("[INFO] - Inserting import data into TABLE single_parent_rate : "+ row["date"])
	if (debug): print(row)
	datepatern = '%Y-%m-%d'
	query = "INSERT INTO single_parent_rate (date,single_parent_rate) VALUES (STR_TO_DATE('%s', '%s'),%s)" % (row['date'], datepatern, float(row['single_parent_rate']))
	if (debug): print("[INFO] - " + query)
	cursor.execute(query)


def initializeFullMoonTable():
	if (verbose) : print ("[INFO] - Creating TABLE fullmoon")
	cursor.execute("""
	CREATE TABLE IF NOT EXISTS fullmoon (
		id_fullmoon int(2) NOT NULL AUTO_INCREMENT,
		date_fullmoon DATE,
		CONSTRAINT pk_id_fullmoon PRIMARY KEY(id_fullmoon)
	) ENGINE=InnoDB ;
	""")

def initializeDemographieLA():
	if (verbose) : print ("[INFO] - Creating TABLE demographie_LA")
	cursor.execute("""
	CREATE TABLE IF NOT EXISTS demographie_LA (
		year varchar(4),
		population int(10)
	) ENGINE=InnoDB ;
	""")

def initializeUnemployementLA():
	if (verbose) : print ("[INFO] - Creating TABLE unemployement_LA")
	cursor.execute("""
	CREATE TABLE IF NOT EXISTS unemployement_LA (
		date DATE,
		unemployement_rate DECIMAL(4,2)
	) ENGINE=InnoDB ;
	""")

def initializeSingleParentRate():
	if (verbose) : print ("[INFO] - Creating TABLE single_parent_rate")
	cursor.execute("""
	CREATE TABLE IF NOT EXISTS single_parent_rate (
		date DATE,
		single_parent_rate DECIMAL(4,2)
	) ENGINE=InnoDB ;
	""")

def initializeNormalizedTable():
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
			crime_code_Description varchar(100) NOT NULL,
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
			id UNSIGNED TINYINT NOT NULL AUTO_INCREMENT,
			Police_Budget UNSIGNED INT(12),
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


def importMoCodes(row):
	if (verbose) : print ("[INFO] - Importing Mo_Codes : "+ row['id_Modus_Operandi'])
	if (debug) : print (row)

	id_Mo = row['id_Modus_Operandi']
	Desc_Mo = row['modus_Operandi_Description']

	query = "INSERT INTO modus_operandi_type (id_modus_operandi,modus_operandi_description )  VALUES (%s, \"%s\")" % (id_Mo, Desc_Mo)

	if (debug) : print (query)
	cursor.execute(query)

def importDescent(row):
	if (verbose) : print ("[INFO] - Importing Descent Value : "+ row['Descent_Description'])
	if (debug) : print (row)

	id_Desc = row['Descent_Code']
	desc_Desc = row['Descent_Description']

	query = "INSERT INTO descent_type (id_descent,descent_description) VALUES (\"%s\", \"%s\")" % (id_Desc, desc_Desc)

	if (debug) : print (query)
	cursor.execute(query)

def extractWeapon():
	if (verbose) : print ("[INFO] - Extracting Weapons from t_crime ")
	cursor.execute("""
			  INSERT INTO weapon_type(id_weapon,weapon_description)
			  SELECT Distinct weapon_used_code, weapon_description
				  FROM t_crime_import;
			""")

def extractPremise():
	if (verbose) : print ("[INFO] - Extracting Premise from t_crime ")
	cursor.execute("""
			  INSERT INTO premise_type(id_premise,premise_description)
			  SELECT Distinct premise_code,premise_description
				  FROM t_crime_import;
			""")

def extractArea() :
	if (verbose) : print ("[INFO] - Extracting Area from t_crime ")
	cursor.execute("""
			  INSERT INTO area(id_area,area_name)
			  SELECT Distinct id_area,area_name
				  FROM t_crime_import;
			""")

def extractCrime() :
	if (verbose) : print ("[INFO] - Extracting Crime_type from t_crime ")

	cursor.execute("""
			  INSERT INTO crime_type(id_crime,crime_code_description)
			  SELECT Distinct crime_code,crime_code_description
				  FROM t_crime_import;
			""")

def importCrimeFact() :
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

def establishConnection() :

	print ("[INFO] - procedure de connexion à la base de donnee MYSQL  ")
	print ("[INFO] - Veuillez pressez ENTER pour valider la valeur par default ")

	inputHost 	= input("[INFO] - Veuillez saisir l' host de la base de donnée (default = "+ defaultHost +") : ")
	inputUser 	= input("[INFO] - Veuillez saisir l' identifiant de connexion à la base de donnée (default = "+ defaultUser +") : ")
	inputPassword 	= input("[INFO] - Veuillez saisir le password de connexion à la base de donnée (default = "+ defaultPassword +") : ")

	if len(inputHost) == 0 : 	inputHost = defaultHost
	if len(inputUser) == 0 : 	inputUser = defaultUser
	if len(inputPassword) == 0 : 	inputPassword = defaultPassword

	return mysql.connector.connect(host=inputHost,user=inputUser,password=inputPassword)

# -------- SCRIPT BEGINS HERE AND WILL EXECUTE FUNCTIONS DECLARED ABOVE

# ---- Connection to Mysql Server

conn = establishConnection()
#conn = mysql.connector.connect(host="localhost",user="root",password="root")

# ---- Init Mysql Cursor
cursor = conn.cursor()

# ---- Creating database
initializeDatabase()
conn.commit()

# ---- Select Database
useDb()

# ---- Clear Database
clearDb()
conn.commit()


# ---- Create Temp Table for inline Import
initializeTableImport()

# ---- create normalized tables
initializeNormalizedTable()
initializeFullMoonTable()
initializeDemographieLA()
initializeUnemployementLA()
initializeSingleParentRate()

# ---- commit Changes into DataBase
conn.commit()


# ---- Read Cleared data imported from US Open Data
#filename = '../source/Crime_Data_from_2010_to_Present.csv'
filename = '../source/CrimeDataLight.csv'
with open(filename) as csvfile:

# ---- Read each line of csv file
	reader = csv.DictReader(csvfile, delimiter=',' , lineterminator ='\n')
	for row in reader:
# ---- Import each row into Mysql Database
		ImportRowInTempTable(row)

conn.commit()

# ---- Infill new Table from t_crime_import
extractArea()
extractWeapon()
extractPremise()
extractCrime()
conn.commit()

# ---- fill Mo Codes data from mo_code.csv
csvLink = '../source/mo_code.csv'
with open(csvLink) as csvLinkRead :
	reader = csv.DictReader(csvLinkRead, delimiter = ';', lineterminator = '\r')
	for row in reader:
		importMoCodes(row)
conn.commit()


# ---- fill Descent Value data from descent_code.csv
csvLink = '../source/descent_code.csv'
with open(csvLink) as csvLinkRead :
	reader = csv.DictReader(csvLinkRead, delimiter = ',')
	for row in reader:
		importDescent(row)
conn.commit()

importCrimeFact()
conn.commit()

# ---- fill Full Moon data from full_moon.csv
csvLink = '../source/full_moon.csv'
with open(csvLink) as csvLinkRead :
	reader = csv.DictReader(csvLinkRead, delimiter = ',')
	for row in reader:
		importFullMoonData(row)
conn.commit()

# ---- fill Demogrphic data from demo_LA.csv
csvLink = '../source/demo_LA.csv'
with open(csvLink) as csvLinkRead :
	reader = csv.DictReader(csvLinkRead, delimiter = ',')
	for row in reader:
		importDemographieLA(row)
conn.commit()

# ---- fill Unemployement Rate data from Unemployement_Rate_LA.csv
csvLink = '../source/Unemployement_Rate_LA.csv'
with open(csvLink) as csvLinkRead :
	reader = csv.DictReader(csvLinkRead, delimiter = ',')
	for row in reader:
		importUnemployementRate(row)
conn.commit()


# ---- fill Single Parent Rate data from demo_LA.csv
csvLink = '../source/SingleParentRate.csv'
with open(csvLink) as csvLinkRead :
	reader = csv.DictReader(csvLinkRead, delimiter = ',')
	for row in reader:
		importSingleParentRate(row)
conn.commit()