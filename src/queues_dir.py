""" модуль каталога очередей сообщений """
from multiprocessing import Queue
from typing import Union

from src.config import CRITICALITY_STR, DEFAULT_LOG_LEVEL, LOG_ERROR, LOG_INFO


class QueuesDirectory:
    """ каталог очередей сообщений """
    log_prefix = "[QUEUES]"
    log_level = DEFAULT_LOG_LEVEL

    def __init__(self):
        self._log_message(LOG_INFO, "создан каталог очередей")

        # словарь с очередями компонентов
        self.queues = {}

    def _log_message(self, criticality: int, message: str):
        """_log_message печатает сообщение заданного уровня критичности

        Args:
            criticality (int): уровень критичности
            message (str): текст сообщения
        """
        if criticality <= self.log_level:
            print(f"[{CRITICALITY_STR[criticality]}]{self.log_prefix} {message}")

    def register(self, queue: Queue, name: str):
        """register регистрация очереди с заданным именем

        Args:
            queue (Queue): очередь
            name (str): имя
        """
        self._log_message(LOG_INFO, f"регистрируем очередь {name}")
        self.queues[name] = queue

    def get_queue(self, name:str) -> Union[Queue, None]:
        """get_queue выдаёт из каталога очередь с указанным именем

        Args:
            name (str): имя очереди

        Returns:
            Union[Queue, None]: очередь или None если такой очереди нет
        """
        try:
            return self.queues[name]
        except KeyError as e:
            self._log_message(LOG_ERROR, f"очередь не найдена {e}")
            return None
