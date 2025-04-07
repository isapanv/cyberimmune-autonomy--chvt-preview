from dataclasses import dataclass
from geopy import Point
from typing import Any, Optional

@dataclass
class Event:
    source: str
    destination: str
    operation: str
    parameters: Any = None
    signature: Optional[str] = None

    def __init__(self, source, destination, operation, parameters=None, **kwargs):
        self.source = source
        self.destination = destination
        self.operation = operation
        self.signature = None
        
        # Обработка параметров
        if parameters is None:
            parameters = {}
            
        # Конвертируем GeoPoint в dict
        if hasattr(parameters, 'latitude'):
            parameters = {
                'latitude': parameters.latitude,
                'longitude': parameters.longitude,
                'altitude': getattr(parameters, 'altitude', 0)
            }
        
        self.parameters = parameters

        # Совместимость со старым кодом
        if 'extra_parameters' in kwargs:
            if not hasattr(self, 'extra_parameters'):
                self.extra_parameters = kwargs['extra_parameters']

    def get_position(self):
        """Извлекает позицию в любом формате"""
        if not isinstance(self.parameters, dict):
            return None
            
        if 'latitude' in self.parameters:
            return Point(
                self.parameters['latitude'],
                self.parameters['longitude'],
                self.parameters.get('altitude', 0)
            )
        return None