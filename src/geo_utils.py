from geopy import Point

def position_to_dict(position):
    """Convert GeoPoint to serializable dict"""
    if isinstance(position, Point):
        return {
            '__geo__': True,
            'lat': position.latitude,
            'lon': position.longitude,
            'alt': position.altitude
        }
    return position

def dict_to_position(data):
    """Convert dict back to GeoPoint"""
    if isinstance(data, dict) and data.get('__geo__'):
        return Point(data['lat'], data['lon'], data['alt'])
    return data
from geopy import Point

def dict_to_geopoint(data):
    """Convert dict back to GeoPoint"""
    if isinstance(data, dict) and data.get('__geopoint__'):
        return Point(data['lat'], data['lon'], data['alt'])
    return data