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
        SELECT crime.`id_crime` as id, date_occurred as date,
                YEAR(`date_occurred`) as year,
                MONTH(`date_occurred`) as month,
                DAY(date_occurred) as day,
                category,
                time_occurred as time FROM `crime`
                INNER JOIN crime_type on crime_code = crime_type.id_crime
        ORDER BY date_occurred, time_occurred
    # """)

    for (id, date, year, month, day, category, time) in cursor:
        if (sun_day < date):
            sun_day = date
            sun = city.sun(date=sun_day, local=True)
            sunrise_hour = str(sun['sunrise'].hour) + str(sun['sunrise'].minute)
            sunset_hour = str(sun['sunset'].hour) + str(sun['sunset'].minute)

        # Day begin atomaticaly during night in LA
        if (str(year) not in crime_during_night):
            str_year = str(year)
            crime_during_day[str_year] = {"Inchoate": 0, "Personnal": 0, "Property": 0, "Statutory":0}
            crime_during_night[str_year] = {"Inchoate": 0, "Personnal": 0, "Property": 0, "Statutory":0}



        if (int(time) > int(sunrise_hour) and int(time) < int(sunset_hour) ):
            crime_during_day[str(year)][str(category)] += 1
            # crime_during_day[str(year)] += 1
        else:
            crime_during_night[str(year)][str(category)] += 1
            # crime_during_night[str(year)] += 1


    print ("Crime de Jour"+str(crime_during_day))
    print ("Crime de Nuit"+str(crime_during_night))


def deployChart(database):
    # ---- Connection to Mysql Server
    conn = database

    # ---- Init Mysql Cursor
    cursor = conn.cursor

    # ---- drawing first chart
    drawChart(cursor)