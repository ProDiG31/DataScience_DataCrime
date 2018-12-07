# Nombre total de crime sexuel : 26587
# Moyenne d'age des victime : 25.0273
# Moyenne d'age group by year :
    # SELECT COUNT(id_crime), AVG(`victim_age`), YEAR(`date_occurred`) as year FROM `crime`
    # where crime_code in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    # OR crime_code_1 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    # OR crime_code_2 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    # OR crime_code_3 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    # OR crime_code_4 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    # Group by year

# SELECT COUNT(id_crime) as nombre, AVG(`victim_age`), YEAR(`date_occurred`) as year, dt.descent_description as gender FROM `crime`
# 	INNER JOIN descent_type as dt ON dt.id_descent = crime.`victim_descent`
#     where crime_code in (121, 122, 815, 820, 821, 822, 830, 840, 860)
#     OR crime_code_1 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
#     OR crime_code_2 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
#     OR crime_code_3 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
#     OR crime_code_4 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
#     Group by year, dt.descent_description

import plotly.plotly as py
import plotly.figure_factory as ff
import numpy as np
import plotly.graph_objs as go


def drawChart(cursor):
    print ("[INFO] - Requesting data for Rape Chart")

    cursor.execute("""SELECT AVG(`victim_age`), YEAR(`date_occurred`) as year, dt.descent_description as gender FROM `crime`
	INNER JOIN descent_type as dt ON dt.id_descent = crime.`victim_descent`
    where crime_code in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    OR crime_code_1 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    OR crime_code_2 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    OR crime_code_3 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    OR crime_code_4 in (121, 122, 815, 820, 821, 822, 830, 840, 860)
    Group by year, dt.descent_description""")

    # Create one line for each gender
    # will contain each years and year will be x
    # and y wil be average
    genders = {}

    for (moyenne, year, gender ) in cursor:
        if (str(gender) not in genders):
            genders[str(gender)] = {}
        if (str(year) not in genders[str(gender)]):
            genders[str(gender)][str(year)] = {}
        genders[str(gender)][str(year)]=moyenne

    chartData = []

    for g in genders:
        x_years = []
        y_moyenne = []
        for f in genders[g]:
           x_years.append(f)
           y_moyenne.append(genders[g][f])
        chartData.append(go.Scatter(
            x = x_years,
            y = y_moyenne,
            mode = 'lines+markers',
            name = str(g)
        ))

    py.plot(chartData, filename='grouped-bar',auto_open=True)

    # print("result" + str(genders) )



def deployChart(database):
    # ---- Connection to Mysql Server
    conn = database

    # ---- Init Mysql Cursor
    cursor = conn.cursor

    # ---- drawing first chart
    drawChart(cursor)