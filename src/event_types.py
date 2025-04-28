""" типы данных для информационных и управляющих сообщений """
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class Event:
    """ формат событий для обработки """
    source: str       # отправитель
    destination: str  # получатель - название очереди блока-получателя, \
    # в которую нужно отправить сообщение
    operation: str    # чего хочет (запрашиваемое действие)
    parameters: Any   # с какими параметрами
    extra_parameters: Any = None      # доп. параметры
    signature: Optional[str] = None   # цифровая подпись или аналог\
                                      # для проверки целостности и аутентичности сообщения


@dataclass
class ControlEvent:
    """ формат управляющих команд для сущностей (например, для остановки работы) """
    operation: str  # код операции
