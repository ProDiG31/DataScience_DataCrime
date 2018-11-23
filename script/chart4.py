import plotly.plotly as py
import plotly.figure_factory as ff
import numpy as np
import plotly.graph_objs as go
import datetime
from astral import Astral

city_name = 'Los Angeles'
a = Astral()
a.solar_depression = 'civil'

city = a[city_name]

# importing database class
from databaseUtil import Database


def drawChart(cursor):

    sun_day = datetime.date(2010, 1, 1)
    sun = city.sun(date=sun_day, local=True)
    sunrise_hour = str(sun['sunrise'].hour) + str(sun['sunrise'].minute)
    sunset_hour = str(sun['sunset'].hour) + str(sun['sunset'].minute)


    crime_during_day={}
    crime_during_night={}

    cursor.execute("""
        SELECT `id_crime` as id, date_occurred as date,
                YEAR(`date_occurred`) as year,
                MONTH(`date_occurred`) as month,
                DAY(date_occurred) as day,
                time_occurred as time FROM `crime`
        ORDER BY date_occurred, time_occurred
    """)

    # print (sunrise_hour)
    # print (sunset_hour)

    for (id, date, year, month, day, time) in cursor:
        if (sun_day < date):
            sun_day = date
            sun = city.sun(date=sun_day, local=True)
            sunrise_hour = str(sun['sunrise'].hour) + str(sun['sunrise'].minute)
            sunset_hour = str(sun['sunset'].hour) + str(sun['sunset'].minute)

        # print (str(year) in crime_during_day)

        if (str(year) not in crime_during_day):
            str_year = str(year)
            crime_during_day[str_year] = 0

        if (str(year) not in crime_during_night):
            crime_during_night[str(year)] = 0

        # print ("Time crime: "+str(time))
        # print ("Sunrise: "+str(sunrise_hour))
        # print ("Sunset: "+str(sunset_hour))
        # print (type(time))
        # print (type(sunrise_hour))
        
        # print (time < sunset_hour)



        if (int(time) > int(sunrise_hour) and int(time) < int(sunset_hour) ):
            crime_during_day[str(year)] += 1
        else:
            crime_during_night[str(year)] += 1
        

    print ("Crime de nuit"+str(crime_during_day))
    print ("Crime de nuit"+str(crime_during_night))



























    # for (nb_crimes, years ) in cursor:
    #     years_day.append(years)
    #     crime_during_day.append(nb_crimes)

    # cursor.execute("""
    #     SELECT count(id_crime), YEAR(date_occurred) as years FROM `crime`
    #     WHERE time_occurred < 700
    #     OR time_occurred > 1900
    #     group by years
    #     ORDER BY years
    # """)

    # for (nb_crimes, years ) in cursor:
    #     years_night.append(years)
    #     crime_during_night.append(nb_crimes)

    # trace1 = go.Bar(
    #     x=years_night,
    #     y=crime_during_night,
    #     name='Crimes de jour, entre 7h et 19h'
    # )
    # trace2 = go.Bar(
    #     x=years_day,
    #     y=crime_during_day,
    #     name='Crimes de nuit, entre 19h et 7h'
    # )

    # data = [trace1, trace2]
    # layout = go.Layout(
    #     barmode='group'
    # )

    # fig = go.Figure(data=data, layout=layout)
    # py.plot(fig, filename='grouped-bar')



def deployChart(database):
    # ---- Connection to Mysql Server
    conn = database

    # ---- Init Mysql Cursor
    cursor = conn.cursor

    # ---- drawing first chart
    drawChart(cursor)