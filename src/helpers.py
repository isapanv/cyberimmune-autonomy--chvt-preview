from geopy import Point

def serialize_position(position):
    """Convert GeoPoint to serializable dict"""
    if position is None:
        return None
    if isinstance(position, dict):
        return position  # Already serialized
    if hasattr(position, 'latitude'):
        return {
            '__geopoint__': True,
            'lat': position.latitude,
            'lon': position.longitude,
            'alt': getattr(position, 'altitude', 0)
        }
    raise ValueError(f"Unsupported position type: {type(position)}")

def deserialize_position(data):
    """Convert dict back to GeoPoint"""
    if data is None:
        return None
    if isinstance(data, Point):
        return data
    if isinstance(data, dict):
        if '__geopoint__' in data:
            return Point(data['lat'], data['lon'], data['alt'])
        if 'latitude' in data:
            return Point(data['latitude'], data['longitude'], data.get('altitude', 0))
    return data