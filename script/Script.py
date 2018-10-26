# coding: utf-8
import setting
# importing database class
from databaseUtil import Database

db = Database()

isDeployed = input("[INFO] - Souhaitez vous deployer la base de donnée (Y/N) :")
if(isDeployed == "Y"):
	from databaseDeploy import deployDatabase
	deployDatabase(db)
else: print("[INFO] - Deploy skipped")

import plotlySetting

from chart2 import deployChart

deployChart(db)
