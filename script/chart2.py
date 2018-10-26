import plotly.plotly as py
import plotly.figure_factory as ff
import numpy as np
import plotly.graph_objs as go

# importing database class
from databaseUtil import Database

# -------- VARIABLES DECLARATION ZONE  ------------------

def drawChart(cursor):
    print ("[INFO] - Requesting data for Chart2")

    # cursor.execute("""
    #     SELECT DISTINCT YEAR(date_occurred) AS years,
    #             (SELECT gravity
    #             FROM crime_type
    #             WHERE crime_code_1 = id_crime) AS gravity,
    #             COUNT(crime_code_1) AS counter
    #     FROM db_datacrime.crime
    #     group by gravity,YEAR(date_occurred)
    #     having gravity != ''
    #     order by YEAR(date_occurred);
    # """)
    cursor.execute("""
        SELECT DISTINCT YEAR(date_occurred) AS years,
            (SELECT category
            FROM crime_type
            WHERE crime_code_1 = id_crime) AS category,
            COUNT(crime_code_1) AS counter
        FROM crime
        group by category,YEAR(date_occurred)
        having category != ''
        order by YEAR(date_occurred)
    """)

    print ("[INFO] - Data Received ")
    crimeGravityData=[]
    data = {}

    #On lit les  données de la reponse de la Requete
    for (years, gravity, counter ) in cursor:
        #On insert dans les tuples (gravité / nombre d'utilisation) dans un tableau dans une hashmap avec pour clé
        if (years in data): #si la clé existe deja
            data[years].append([gravity,counter]) #On ajoute dans le tableau avec pour clé de l'année dans la hashmap
        else:
            # print('newkeys')
            data[years] = [[gravity,counter]] #On crée la clé de l'année avec pour valeur un tuple dans la hashmap

        if (gravity not in crimeGravityData):#On ajoute le type d'arme dans la liste des types d'armes si el n'existe pas
            crimeGravityData.append(gravity)

    # pour chaque tableau de tuple la hashmap pour clé l'année
    # chartData = []
    group_label = []
    z = []
    for yearz in data:
        # print(str(yearz) + " = " + str(data[yearz]))
        group_label.append(yearz)
        yearData = []
        for dataRead in data[yearz]:
            yearData.insert(crimeGravityData.index(dataRead[0]),dataRead[1])
        z.insert(group_label.index(yearz),yearData)

    colorscale = [
        [0, 'rgb(124,252,0)'],
        [1.0, 'rgb(255,0,0)']
    ]

    trace = go.Heatmap( z=z,
                        x=crimeGravityData,
                        y=group_label,
                        colorscale=colorscale)
    data=[trace]
    py.plot(data,
            filename='labelled-heatmap',
            auto_open=True)
    # fig['layout'].update(title='Count Used weapons into crime by weapons time on years')
        # title='Count Used weapons into crime by weapons time on years',

def deployChart(database):
    # ---- Connection to Mysql Server
    conn = database

    # ---- Init Mysql Cursor
    cursor = conn.cursor

    # ---- drawing first chart
    drawChart(cursor)