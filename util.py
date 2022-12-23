from geopandas.tools import geocode  # type: ignore
from dadata import Dadata  # type: ignore
import geocoder
from geopy.geocoders import Nominatim
import config as cfg

# TOKEN = cfg.DADATA_API
# SECRET = cfg.SECRET_KEY

'''
lib works only with big cities and states capitals
do not works with cities of Russia
'''
def get_location(location_name: str):
    # get long and lat values by city name
    location = geocode(location_name, provider="nominatim", user_agent='my_request')
    point = location.geometry.iloc[0]
    long = point.x
    lat = point.y
    list_loc = [long, lat]
    return list_loc


def get_loc_dadata(loc: str):
    token = "ecb03b25f5668769ab7d6d622af38fd3ad23acfd"
    secret = "db2f0db46f45b1bbbb4852fbbdcb6b1587b5777a"
    dadata = Dadata(token, secret)
    result = dadata.clean(name="address", source=loc)
    lat = result.get('geo_lat')
    long = result.get('geo_lon')
    location = {'lat': lat, 'long': long}
    # print(location)
    return location


def get_loc_geopy(address: str):
    geolocator = Nominatim(user_agent="my_request")
    location = geolocator.geocode(address)
    lat = location.latitude
    long = location.longitude
    result = {'lat': lat, 'long': long}
    return result



if __name__=='__main__':
    # test
    # print(get_loc_dadata('ufa').get('lat'))
    print(get_loc_geopy('tula, Russia'))
