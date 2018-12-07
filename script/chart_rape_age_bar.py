# Demographie a comparer : https://censusreporter.org/profiles/16000US0644000-los-angeles-ca/


import plotly.plotly as py
import plotly.figure_factory as ff
import numpy as np
import plotly.graph_objs as go


def drawChart(cursor):
    print ("[INFO] - Requesting data for Rape Pie Chart")

    cursor.execute("""SELECT victim_age as age FROM `crime`
	where crime_code in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    OR crime_code_1 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    OR crime_code_2 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    OR crime_code_3 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    OR crime_code_4 in (121, 122, 815, 820, 821, 822, 830, 840, 860)""")

    # Create one line for each gender
    # will contain each years and year will be x
    # and y wil be average
    labels = ["0", "1-9", "10-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80+"]
    values = [0,0,0,0,0,0,0,0,0,0]

    for (age) in cursor:
        if int(age[0]) > 20 and int(age[0]) < 29:
            values[3] = values[3]+1
        elif int(age[0]) > 30 and int(age[0]) < 39:
            values[4] = values[4]+1
        elif int(age[0]) > 10 and int(age[0]) < 19:
            values[2] = values[2]+1
        elif int(age[0]) > 40 and int(age[0]) < 49:
            values[5] = values[5]+1
        elif int(age[0]) > 50 and int(age[0]) < 59:
            values[6] = values[6]+1
        elif int(age[0]) > 60 and int(age[0]) < 69:
            values[7] = values[7]+1
        elif int(age[0]) > 70 and int(age[0]) < 79:
            values[8] = values[8]+1
        elif int(age[0]) >= 1 and int(age[0]) < 9:
            values[1] = values[1]+1
        elif int(age[0]) == 0:
            values[0] = values[0]+1
        elif int(age[0]) > 80:
            values[9] = values[9]+1

    data = [go.Bar(
            x=labels,
            y=values
    )]

    py.plot(data, filename='basic-bar',auto_open=True)



def deployChart(database):
    # ---- Connection to Mysql Server
    conn = database

    # ---- Init Mysql Cursor
    cursor = conn.cursor

    # ---- drawing first chart
    drawChart(cursor)