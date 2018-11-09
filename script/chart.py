import plotly.plotly as py
import plotly.figure_factory as ff
import numpy as np
import plotly.graph_objs as go
# import os

# importing database class
from databaseUtil import Database

# -------- VARIABLES DECLARATION ZONE  ------------------

# databaseName = os.getenv("databaseName")

def drawChart(cursor):
    print ("[INFO] - Requesting data for Chart1")

    # cursor.execute("""
    #         SELECT count(Weapon_Used_Code) as countUse, (
    #             SELECT weapon_Description
    #             FROM weapon_type
    #             WHERE Weapon_Used_Code = id_Weapon) as weapon_type
    #         FROM db_datacrime.t_crime_import
    #         group by Weapon_Used_Code
    #         order by countUse desc;
    #         """)

    cursor.execute("""
        SELECT weapon_description as weapon_type,
            YEAR(`date_occurred`) as years,
            count(id_crime) counter
        From crime INNER JOIN weapon_type
        ON `weapon_used_code`=`id_weapon`
        GROUP BY years, weapon_type
    """)

    print ("[INFO] - Data Received ")
    weaponsTypeData=[]
    data = {}

    #On lit les  données de la reponse de la Requete
    for (years, weapon_type, counter ) in cursor:
        #On insert dans les tuples (type d'arme / nombre d'utilisation) dans un tableau dans une hashmap avec pour clé
        if (years in data): #si la clé existe deja
            # print('append')
            data[years].append([weapon_type,counter]) #On ajoute dans le tableau avec pour clé de l'année dans la hashmap
        else:
            # print('newkeys')
            data[years] = [[weapon_type,counter]] #On crée la clé de l'année avec pour valeur un tuple dans la hashmap

        if (weapon_type not in weaponsTypeData):#On ajoute le type d'arme dans la liste des types d'armes si el n'existe pas
            weaponsTypeData.append(weapon_type)

    # pour chaque tableau de tuple la hashmap pour clé l'année
    chartData = []
    group_label = []
    for yearz in data:
        print(str(yearz) + " = " + str(data[yearz]))
        group_label.append(yearz)
        x = weaponsTypeData
        y = []

        for dataRead in data[yearz]:
            y.insert(weaponsTypeData.index(dataRead[0]),dataRead[1])
        print ("X = " + str(x))
        print ("Y = " + str(y))
        chartData.append(go.Bar(
            x=x,
            y=y,
            name=yearz
        ))
    layout = go.Layout(
        barmode='group'
    )

    # fig['layout'].update(title='Count Used weapons into crime by weapons time on years')
        # title='Count Used weapons into crime by weapons time on years',

    fig = go.Figure(
        data=chartData,
        layout=layout)
    py.plot(fig, filename='grouped-bar',auto_open=True)

    # py.plot(fig, filename='Distplot Colors', auto_open=True)
    # py.plot(data, filename = 'basic-line', auto_open=True)

def deployChart(database):
    # ---- Connection to Mysql Server
    conn = database

    # ---- Init Mysql Cursor
    cursor = conn.cursor

    # ---- drawing first chart
    drawChart(cursor)