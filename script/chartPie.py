import plotly.plotly as py
import plotly.figure_factory as ff
import numpy as np
import plotly.graph_objs as go

# importing database class
from databaseUtil import Database

# -------- VARIABLES DECLARATION ZONE  ------------------
chartData = []
crimeCount = []
weaponTypeLabel=[]
annotationsChart = []

positionChart = [
    {'x': [0, .2]   , 'y': [.7, 1]},
    {'x': [.25, .45], 'y': [.7, 1]},
    {'x': [.5, .70] , 'y': [.7, 1]},
    {'x': [.75, .95], 'y': [.7, 1]},

    {'x': [0, .2]   , 'y': [.33, .66]},
    {'x': [.25, .45], 'y': [.33, .66]},
    {'x': [.5, .70] , 'y': [.33, .66]},
    {'x': [.75, .95], 'y': [.33, .66]},

    {'x': [0, .2]   , 'y': [0, .3]},
    {'x': [.25, .45], 'y': [0, .3]},
    {'x': [.5, .70] , 'y': [0, .3]},
    {'x': [.75, .95], 'y': [0, .3]}
]

positionIndex = 0
columnNumber = 4

def getCrimeCount(cursor):
    print ("[INFO] - Requesting data for Chart Pie Count")

    cursor.execute("""
        SELECT YEAR(date_occurred) as year,
                COUNT(id_crime) as count
        FROM crime
        where crime.weapon_used_code != 0
        group by YEAR(date_occurred)
        """)

    for (year,count) in cursor:
        crimeCount.append([year,count])

def getWeaponLabel(cursor):
    print ("[INFO] - Requesting Weapons Label for Chart Pie : ")

    cursor.execute("""
        SELECT weapon_description
        FROM weapon_type
        WHERE id_weapon!= 0
    """ )

    weaponType = []

    # print (cursor)
    for weapon_description in cursor:
        weaponType.append(str(weapon_description)[2:-3])

    return weaponType

def retrieveData(cursor,year,totalCount,index, weaponType):
    print ("[INFO] - Requesting data for Chart Pie : " + str(year))

    cursor.execute("""
         SELECT weapon_type.weapon_description as weapon,
                COUNT(id_crime) * 100 / (
				SELECT COUNT(id_crime) as counter
				FROM crime
				where crime.weapon_used_code != 0
                AND YEAR(date_occurred) = "%d"
				group by YEAR(date_occurred)
				) as percent
        FROM crime
        Inner Join weapon_type on crime.weapon_used_code=weapon_type.id_weapon
        WHERE YEAR(date_occurred) = "%d"
		AND crime.weapon_used_code != 0
        GROUP BY weapon_used_code
    """ % (year,year))

    print ("[INFO] - Data Received ")

    data = {}

    #On lit les  données de la reponse de la Requete
    for (weapon, percent) in cursor:
        data[weapon] = float(percent) #On crée la clé de l'année avec pour valeur un tuple dans la hashmap

    dataValues = []
    for label in weaponType:
        if(label in data):
            dataValues.append(data[label])
        else:
            dataValues.append(None)

    chartData.append(
         {
        "values": dataValues,
        "labels": weaponType,
        "domain": positionChart[index],
        "name": str(year),
        "hoverinfo":"label+percent+name",
        "textposition": 'inside',
        "hole": .4,
        "type": "pie"
        }
    )

    annotationsChart.append(
        {
            "font": {
                "size": 20
            },
            "showarrow": False,
            "text": year,
            "x": positionChart[index].get("x")[0],
            "y": positionChart[index].get("y")[1]
        }
    )

def drawChart(cursor):
    print ("[INFO] - drawing Chart Pie")

    print (chartData)
    fig = {
        "data" : chartData,
        "layout": {
        "title":"Weapon Type 2011-2018",
        "annotations": annotationsChart
        }
    }
    # py.iplot(fig, filename='')

    py.plot(fig,
            filename='donut',
            auto_open=True)

def deployChart(database):
    # ---- Connection to Mysql Server
    conn = database

    # ---- Init Mysql Cursor
    cursor = conn.cursor

    getCrimeCount(cursor)
    weaponTypeLabel = getWeaponLabel(cursor)

    positionIndex = 0
    for (years, count) in crimeCount:
        # print(str(positionIndex) + " : " + str(years))
        retrieveData(cursor,years,count, positionIndex, weaponTypeLabel)
        positionIndex += 1
    # ---- drawing first chart
    drawChart(cursor)