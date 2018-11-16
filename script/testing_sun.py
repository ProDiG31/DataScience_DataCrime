import datetime
from astral import Astral

city_name = 'Los Angeles'

a = Astral()
a.solar_depression = 'civil'

city = a[city_name]

print('Information for %s/%s\n' % (city_name, city.region))
# Information for London/England

timezone = city.timezone
print('Timezone: %s' % timezone)
# Timezone: Europe/London

print('Latitude: %.02f; Longitude: %.02f\n' % \
    (city.latitude, city.longitude))
# Latitude: 51.60; Longitude: 0.08

sun = city.sun(date=datetime.date(2018, 11, 16), local=True)
# print('Dawn:    %s' % str(sun['dawn']))
print('Sunrise: %s%s' % (str(sun['sunrise'].hour) , str(sun['sunrise'].minute)))
print('Sunrise: %s%s' % (str(sun['sunset'].hour) , str(sun['sunset'].minute)))
# print('Noon:    %s' % str(sun['noon']))
# print('Sunset:  %s' % str(sun['sunset']))
# print('Dusk:    %s' % str(sun['dusk']))