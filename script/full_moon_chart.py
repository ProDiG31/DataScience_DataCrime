import plotly.plotly as py
import plotly.figure_factory as ff
import numpy as np
import plotly.graph_objs as go


# importing database class
from databaseUtil import Database

def drawMoonChart(cursor)

    

    trace_normal_moyenne = go.Bar(
        x=
        y=
        name = "Nombre de crime moyen (hors pleine lune)"
    )

    trace_full_moon = go.Bar(
        # Nombre de crime
        x=

        # Year
        y=
        name = "Nombre de crime lors de pleine lune"
    )




def deployMoonChart(database):
    # ---- Connection to Mysql Server
    conn = database

    # ---- Init Mysql Cursor
    cursor = conn.cursor

    # ---- drawing first chart
    drawChart(cursor)