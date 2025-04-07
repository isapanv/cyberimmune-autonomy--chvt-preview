def create_sitl_event(source, destination, operation, position, **extra):
    """Create events compatible with both SITL and security system"""
    event = Event(
        source=source,
        destination=destination,
        operation=operation,
        parameters={
            'position': {
                'latitude': position.latitude,
                'longitude': position.longitude,
                'altitude': position.altitude
            },
            **extra
        }
    )
    return event