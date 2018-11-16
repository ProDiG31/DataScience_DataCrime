import plotly.plotly as py
import plotly.figure_factory as ff
import numpy as np
import plotly.graph_objs as go

# importing database class
from databaseUtil import Database

def drawChart(cursor):

    crime_during_day=[]
    crime_during_night=[]
    years_day=[]
    years_night=[]

    cursor.execute("""
        SELECT count(id_crime) as nb_crimes, YEAR(date_occurred) AS years FROM `crime`
        WHERE time_occurred > 700
        OR time_occurred < 1900
        GROUP BY years
        ORDER BY years
    """)

    for (nb_crimes, years ) in cursor:
        years_day.append(years)
        crime_during_day.append(nb_crimes)

    cursor.execute("""
        SELECT count(id_crime), YEAR(date_occurred) as years FROM `crime`
        WHERE time_occurred < 700
        OR time_occurred > 1900
        group by years
        ORDER BY years
    """)

    for (nb_crimes, years ) in cursor:
        years_night.append(years)
        crime_during_night.append(nb_crimes)

    trace1 = go.Bar(
        x=years_night,
        y=crime_during_night,
        name='Crimes de jour, entre 7h et 19h'
    )
    trace2 = go.Bar(
        x=years_day,
        y=crime_during_day,
        name='Crimes de nuit, entre 19h et 7h'
    )

    data = [trace1, trace2]
    layout = go.Layout(
        barmode='group'
    )

    fig = go.Figure(data=data, layout=layout)
    py.plot(fig, filename='grouped-bar')



def deployChart(database):
    # ---- Connection to Mysql Server
    conn = database

    # ---- Init Mysql Cursor
    cursor = conn.cursor

    # ---- drawing first chart
    drawChart(cursor)