import plotly.plotly as py
import plotly.figure_factory as ff
import numpy as np
import plotly.graph_objs as go

# importing database class
from databaseUtil import Database

def drawChart(cursor):
    print ("[INFO] - Requesting data for Chart3")


    # print ("[INFO] - Data Received ")
    crime_x = []
    budget_x = []
    data_crime_number = []
    data_budget_number = []

    cursor.execute("""
        SELECT YEAR(date_occurred) as years, count(id_crime) as counter FROM `crime`
        GROUP By years
        ORDER BY years
    """)

    for (years, counter ) in cursor:
        crime_x.append(years)
        data_crime_number.append(counter)

    cursor.execute("""
        SELECT `year_budget` as years, `police_budget` as counter FROM `police_budgets`
    """)

    for (years, counter ) in cursor:
        budget_x.append(years)
        data_budget_number.append(counter)


    crimes = go.Scatter(
        x=crime_x,
        y=data_crime_number,
        name='Crimes',
        fill='tozeroy'
    )
    budget = go.Scatter(
        x=budget_x,
        y=data_budget_number,
        name='Budget',
        fill='tonexty',
        yaxis='y2'
    )

    layout = go.Layout(
        title='Budget vs Crime',
        yaxis=dict(
            title='crimes'
        ),
        yaxis2=dict(
            title='budget',
            titlefont=dict(
                color='rgb(148, 103, 189)'
            ),
            tickfont=dict(
                color='rgb(148, 103, 189)'
            ),
            overlaying='y',
            side='right'
        )
    )

    data = [crimes, budget]
    fig = go.Figure(data=data, layout=layout)
    py.plot(fig, filename='basic-area', auto_open=True)


def deployChart(database):
    # ---- Connection to Mysql Server
    conn = database

    # ---- Init Mysql Cursor
    cursor = conn.cursor

    # ---- drawing first chart
    drawChart(cursor)