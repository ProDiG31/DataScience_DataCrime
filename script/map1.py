import plotly.plotly as py
import plotly.graph_objs as graph_objs
import os

mapbox_access_token = os.getenv("mapbox_api_key")

def drawMap(cursor):

    print ("[INFO] - Requesting data for Map1")

    cursor.execute("""
        SELECT  id_crime,
                YEAR(date_reported) as yearz,
                location
        FROM db_datacrime.crime
        WHERE YEAR(date_reported) = 2015;
    """)

    print ("[INFO] - Data Received ")

    dataLon = []
    dataLat = []
    dataLab = []

    for (id_crime, yearz, location ) in cursor:

        locationStr = location.replace('(','').replace(')','').split(',')
        dataLat.append(locationStr[0])
        dataLon.append(locationStr[1])
        dataLab.append(str(str(id_crime) + ' - ' + str(yearz)))
        # input("wait")
        # dataSet.append({
        #     "type": "Feature",
        #     "geometry": {
        #         "type": "Point",
        #         "coordinates": str(locationStr).replace('\'','')
        #     },
        #     "properties": {
        #         "name": str(str(id_crime) + ' - ' + str(yearz))
        #     }
        # })

    data = graph_objs.Data([
        graph_objs.Scattermapbox(
            lat=dataLat,
            lon=dataLon,
            mode='markers',
            marker=dict(
                size=9
            ),
            text=dataLab
        )
    ])
    layout = graph_objs.Layout(
        height=600,
        autosize=True,
        hovermode='closest',
        mapbox=dict(
            # layers=[
            #     dict(
            #         # sourcetype = 'geojson',
            #         # source = dataSet,
            #         # type = 'fill',
            #         # color = 'rgba(163,22,19,0.8)'
            #          lat=38.92,
            #          lon=-77.07
            #     )
                # ,
                # dict(
                #     sourcetype = 'geojson',
                #     source = 'https://raw.githubusercontent.com/plotly/datasets/master/florida-blue-data.json',
                #     type = 'fill',
                #     color = 'rgba(40,0,113,0.8)'
                # )
            # ],
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