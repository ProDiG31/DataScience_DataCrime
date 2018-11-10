import plotly.plotly as py
import plotly.graph_objs as graph_objs
import os

mapbox_access_token = os.getenv("mapbox_api_key")

def drawMap(cursor):

    print ("[INFO] - Requesting data for Map1")

    cursor.execute("""
        SELECT
            crime_type.gravity as gravity,
            YEAR(crime.date_occurred) as years,
            location
        FROM crime
        Inner Join crime_type on crime.crime_code=crime_type.id_crime
        HAVING years = 2015
        ORDER BY years; """)

    print ("[INFO] - Data Received ")

    dataLonFel = []
    dataLatFel = []
    dataLabFel = []

    dataLonMis = []
    dataLatMis = []
    dataLabMis = []

    for (gravity, years, location) in cursor:
        # Misdemeanor
        # Felony

        locationStr = location.replace('(','').replace(')','').split(',')
        if (gravity == "Misdemeanor"):
            dataLatMis.append(locationStr[0])
            dataLonMis.append(locationStr[1])
            dataLabMis.append(str(gravity + ' - ' + str(years)))
        elif(gravity == "Felony"):
            dataLatFel.append(locationStr[0])
            dataLonFel.append(locationStr[1])
            dataLabFel.append(str(gravity + ' - ' + str(years)))

    # print(dataLabFel)
    # print(dataLabMis)

    data = graph_objs.Data([
        graph_objs.Scattermapbox(
            lat=dataLatMis,
            lon=dataLonMis,
            mode='markers',
            marker=dict(
                size=9,
                color='rgb(255, 0, 0)',
                opacity=0.7
            ),
            text=dataLabMis
        ),
         graph_objs.Scattermapbox(
            lat=dataLatFel,
            lon=dataLonFel,
            mode='markers',
            marker=dict(
                size=9,
                color='rgb(0, 255, 0)',
                opacity=0.7
            ),
            text=dataLabFel
        )
    ])
    layout = graph_objs.Layout(
        height=600,
        autosize=True,
        hovermode='closest',
        mapbox=dict(
            accesstoken=mapbox_access_token,
            bearing=0,
            center=dict(
                lat=34.1,
                lon=-118.30
            ),
            pitch=0,
            zoom=10,
            style='light'
        ),
    )

    fig = dict(data=data, layout=layout)
    py.plot(fig, filename='county-level-choropleths-python')

def deployMap(database):
        # ---- Connection to Mysql Server
    conn = database

    # ---- Init Mysql Cursor
    cursor = conn.cursor

    # ---- drawing first chart
    drawMap(cursor)