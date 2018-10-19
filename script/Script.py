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
	print("[INFO] - d_vigne database Selected")
	cursor.execute("USE %s;" % database ) 

def clearDb():
	print ("[INFO] - Dropping Previous Tables")
	cursor.execute("DROP TABLE IF EXISTS %s;" % table_import_temp)
	cursor.execute("DROP TABLE IF EXISTS crime;") 
	cursor.execute("DROP TABLE IF EXISTS area;") 
	cursor.execute("DROP TABLE IF EXISTS weapon_type;") 
	cursor.execute("DROP TABLE IF EXISTS crime_type;") 
	cursor.execute("DROP TABLE IF EXISTS premise_type;") 
	cursor.execute("DROP TABLE IF EXISTS modus_Operandi_type;") 
	cursor.execute("DROP TABLE IF EXISTS status_type;") 
	cursor.execute("DROP TABLE IF EXISTS descent_type;") 

def initializeTableImport():
	print ("[INFO] - Creating TABLE %s" % table_import_temp)  
	cursor.execute("""
	CREATE TABLE IF NOT EXISTS t_crime_import (
		DR_Number varchar(9),
		Date_Reported DATE,
		Date_Occurred DATE,
		Time_Occurred varchar(4),
		Area_ID  int(2),
		Area_Name varchar(100),
		Reporting_District int(4),
		Crime_Code int(3),
		Crime_Code_Description varchar(100),
		MO_Codes text,
		Victim_Age int(3),
		Victim_Sex varchar(1),
		Victim_Descent varchar(1),
		Premise_Code int(3),
		Premise_Description varchar(100),
		Weapon_Used_Code int(3),
		Weapon_Description varchar(100),
		Status_Code varchar(5),
		Status_Description varchar(100),
		Crime_Code_1 varchar(100),
		Crime_Code_2 varchar(100),
		Crime_Code_3 varchar(100),
		Crime_Code_4 varchar(100),
		Address varchar(100),
		Cross_Street varchar(100),
		Location varchar(100),
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
		Date_Reported,
		Date_Occurred,
		Time_Occurred,
		Area_ID,
		Area_Name,
		Reporting_District,
		Crime_Code,
		Crime_Code_Description,
		MO_Codes,
		Victim_Age,
		Victim_Sex,
		Victim_Descent,
		Premise_Code,
		Premise_Description,
		Weapon_Used_Code,
		Weapon_Description,
		Status_Code,
		Status_Description,
		Crime_Code_1,
		Crime_Code_2,
		Crime_Code_3,
		Crime_Code_4,
		Address,
		Cross_Street,
		Location
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


def initializeNormalizedTable():
# 	print ("[INFO] - Creating TABLE crime, area, weapon_type, crime_type, premise_type, modus_Operandi_type, status_type and descent_typeDescend_type ("[INFO] - Creating TABLE crime, area, weapon_type, crime_type, premise_type, modus_Operandi_type, status_type and descent_type ")
	cursor.execute("""
	       CREATE TABLE area (
		id_Area int(2) NOT NULL, 
		area_Name varchar(100) NOT NULL, 
		CONSTRAINT pk_id_Area PRIMARY KEY(id_Area)
	) ENGINE=InnoDB;
	""")

	cursor.execute("""
		CREATE TABLE weapon_type (
			id_Weapon int(3) NOT NULL,
			weapon_Description varchar(100) NOT NULL,
			CONSTRAINT pk_id_Weapon PRIMARY KEY(id_Weapon)
		) ENGINE=InnoDB;
	""")
	
	cursor.execute("""
		CREATE TABLE crime_type (
			id_Crime int(3) NOT NULL,
			crime_Code_Description varchar(100) NOT NULL,
			CONSTRAINT pk_id_Crime PRIMARY KEY(id_Crime)
		) ENGINE=InnoDB;
	""")

	cursor.execute("""
		CREATE TABLE premise_type (
			id_Premise int(3) NOT NULL,
			premise_Description varchar(100) NOT NULL,
			CONSTRAINT pk_id_Premise PRIMARY KEY(id_Premise)
		) ENGINE=InnoDB;
	""")
	
	cursor.execute("""
		CREATE TABLE modus_operandi_type (
			id_Modus_Operandi int(3) NOT NULL,
			modus_Operandi_Description varchar(200) NOT NULL,
			CONSTRAINT pk_id_Modus_Operandi PRIMARY KEY(id_Modus_Operandi)
		) ENGINE=InnoDB;
	""")
	
	cursor.execute("""
		CREATE TABLE status_type (
			id_Status varchar(5) NOT NULL,
			status_Description varchar(100) NOT NULL,
			CONSTRAINT pk_id_Status PRIMARY KEY(id_Status)
		) ENGINE=InnoDB;
	""")
	
	cursor.execute("""
		CREATE TABLE descent_type (
			id_Descent varchar(1) NOT NULL,
			descent_Description varchar(100) NOT NULL,
			CONSTRAINT pk_id_Descent PRIMARY KEY(id_Descent)
		) ENGINE=InnoDB;
	""")
	
	cursor.execute("""
		CREATE TABLE crime (
			id_crime varchar(9),
			Date_Reported DATE,
			Date_Occurred DATE,
			Time_Occurred varchar(4),
			Area_ID  int(2),
			Reporting_District int(4),
			Crime_Code int(3),
			MO_Codes varchar(200),
			Victim_Age int(3),
			Victim_Sex varchar(1),
			Victim_Descent varchar(1),
			Premise_Code int(3),
			Weapon_Used_Code int(3),
			Status_Code varchar(5),
			Crime_Code_1 varchar(100),
			Crime_Code_2 varchar(100),
			Crime_Code_3 varchar(100),
			Crime_Code_4 varchar(100),
			Address varchar(100),
			Cross_Street varchar(100),
			Location varchar(100),     
			CONSTRAINT pk_id_crime PRIMARY KEY (id_crime)
		) ENGINE=InnoDB;
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

	query = "INSERT INTO modus_operandi_type (id_Modus_Operandi,modus_Operandi_Description )  VALUES (%s, \"%s\")" % (id_Mo, Desc_Mo)
	
	if (debug) : print (query) 
	cursor.execute(query)

def importDescent(row): 
	if (verbose) : print ("[INFO] - Importing Descent Value : "+ row['Descent_Description'])
	if (debug) : print (row)

	id_Desc = row['Descent_Code']
	desc_Desc = row['Descent_Description']

	query = "INSERT INTO descent_type (id_Descent,descent_Description) VALUES (\"%s\", \"%s\")" % (id_Desc, desc_Desc)
	
	if (debug) : print (query) 
	cursor.execute(query)
	
def extractWeapon(): 
	print ("[INFO] - Extracting Weapons from t_crime ")
	cursor.execute("""
			  INSERT INTO weapon_type(id_Weapon,weapon_Description) 
			  SELECT Distinct Weapon_Used_Code, Weapon_Description
				  FROM t_crime_import;
			""")  

def extractPremise(): 
	print ("[INFO] - Extracting Premise from t_crime ")
	cursor.execute("""
			  INSERT INTO premise_type(id_Premise,premise_Description) 
			  SELECT Distinct Premise_Code,Premise_Description
				  FROM t_crime_import;
			""")  

def extractArea() :
	print ("[INFO] - Extracting Area from t_crime ")
	cursor.execute("""
			  INSERT INTO area(id_area,area_Name) 
			  SELECT Distinct Area_ID,Area_Name
				  FROM t_crime_import;
			""")  

def extractCrime() :
	print ("[INFO] - Extracting Crime_type from t_crime ")
	print ("[INFO] - Extracting Crime_type from t_crime ")
	
	cursor.execute("""
			  INSERT INTO crime_type(id_Crime,crime_Code_Description) 
			  SELECT Distinct Crime_Code,Crime_Code_Description
				  FROM t_crime_import;
			""")  		

def importCrimeFact() : 
	print ("[INFO] - Normalise declaration from t_crime_import ")	
	cursor.execute("""
			  INSERT INTO crime (
							id_crime,
							Date_Reported,
							Date_Occurred,
							Time_Occurred,
							Area_ID,
							Reporting_District,
							Crime_Code,
							MO_Codes,
							Victim_Age,
							Victim_Sex,
							Victim_Descent,
							Premise_Code,
							Weapon_Used_Code,
							Status_Code,
							Crime_Code_1,
							Crime_Code_2,
							Crime_Code_3,
							Crime_Code_4,
							Address,
							Cross_Street,
							Location
							)   
			  SELECT 
					DR_Number,
					Date_Reported,
					Date_Occurred,
					Time_Occurred,
					Area_ID,
					Reporting_District,
					Crime_Code,
					MO_Codes,
					Victim_Age,
					Victim_Sex,
					Victim_Descent,
					Premise_Code,
					Weapon_Used_Code,
					Status_Code,
					Crime_Code_1,
					Crime_Code_2,
					Crime_Code_3,
					Crime_Code_4,
					Address,
					Cross_Street,
					Location
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

conn = establishConnection();
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
conn.commit()

# ---- create normalized tables
initializeNormalizedTable()
conn.commit()


# ---- Read Cleared data imported from US Open Data
#filename = '../source/Crime_Data_from_2010_to_Present.csv'
filename = '../source/CrimeDataLight2.csv'
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
