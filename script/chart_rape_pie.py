# Demographie a comparer : https://censusreporter.org/profiles/16000US0644000-los-angeles-ca/


import plotly.plotly as py
import plotly.figure_factory as ff
import numpy as np
import plotly.graph_objs as go


def drawChart(cursor):
    print ("[INFO] - Requesting data for Rape Pie Chart")

    cursor.execute("""SELECT COUNT(id_crime) as number, dt.descent_description as gender FROM `crime`
	INNER JOIN descent_type as dt ON dt.id_descent = crime.`victim_descent`
    where crime_code in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    OR crime_code_1 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    OR crime_code_2 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    OR crime_code_3 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    OR crime_code_4 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    Group by dt.descent_description""")

    # Create one line for each gender
    # will contain each years and year will be x
    # and y wil be average
    labels = []
    values = []

    for (number, gender ) in cursor:
        labels.append(gender)
        values.append(number)

    trace = go.Pie(labels=labels, values=values)
    py.plot([trace], filename='basic_pie_chart',auto_open=True)



def deployChart(database):
    # ---- Connection to Mysql Server
    conn = database

    # ---- Init Mysql Cursor
    cursor = conn.cursor

    # ---- drawing first chart
    drawChart(cursor)