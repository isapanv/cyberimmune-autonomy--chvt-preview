from abc import abstractmethod
from multiprocessing import Process
from typing import Any
from src.event_types import Event

class BaseBlackBox(Process):
    """Базовый класс для черного ящика, обеспечивающего безопасное хранение журналов"""
    
    def __init__(self):
        super().__init__()
        self._quit = False
    
    @abstractmethod
    def _log_event(self, event: Event, signature: str) -> bool:
        """Абстрактный метод для логирования события с проверкой подписи
        
        Args:
            event: событие для логирования
            signature: цифровая подпись события
            
        Returns:
            bool: True если подпись верна и событие записано, иначе False
        """
        pass
    
    def stop(self):
        """Остановка процесса черного ящика"""
        self._quit = True
    
    def run(self):
        """Основной цикл процесса"""
        while not self._quit:
            # Здесь может быть периодическая проверка очереди событий
            pass