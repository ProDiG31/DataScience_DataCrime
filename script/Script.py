# coding: utf-8
import setting
import os
# importing database class
from databaseUtil import Database

db = Database()

if(os.getenv('DEPLOY')):
	from databaseDeploy import deployDatabase
	deployDatabase(db)
else: print("[INFO] - Deploy skipped")

import plotlySetting

from chart import deployChart
deployChart(db)

from chart2 import deployChart
deployChart(db)

from chart3 import deployChart
deployChart(db)

from chart4 import deployChart
deployChart(db)

from chartPie import deployChart
deployChart(db)

from map1 import deployMap
deployMap(db)

