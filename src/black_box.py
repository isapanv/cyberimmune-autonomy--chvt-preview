from abc import abstractmethod
from queue import Empty

from time import sleep

from multiprocessing import Queue, Process
from typing import Any

from src.event_types import Event, ControlEvent
from src.config import *

class BaseBlackBox(Process):
    """Базовый класс для черного ящика, обеспечивающего безопасное хранение журналов"""
    """ класс монитора безопасности """
    log_prefix = "[SECURITY]"
    event_source_name = BLACK_BOX_QUEUE_NAME
    events_q_name = event_source_name
    log_level = DEFAULT_LOG_LEVEL

    def __init__(self):
        super().__init__()
        
        # инициализируем интервал обновления
        self._control_q = Queue()        
        self._recalc_interval_sec = 0.5
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

    def _log_message(self, criticality: int, message: str):
        """_log_message печатает сообщение заданного уровня критичности

        Args:
            criticality (int): уровень критичности
            message (str): текст сообщения
        """
        if criticality <= self.log_level:
            print(f"[{CRITICALITY_STR[criticality]}]{self.log_prefix} {message}")

    def stop(self):
        """Остановка процесса черного ящика"""
        self._quit = True

    def _check_control_q(self):
        """_check_control_q проверка наличия новых управляющих команд
        """
        try:
            request: ControlEvent = self._control_q.get_nowait()
            self._log_message(LOG_DEBUG, f"проверяем запрос {request}")
            if isinstance(request, ControlEvent) and request.operation == 'stop':
                # поступил запрос на остановку монитора, поднимаем "красный флаг"
                self._quit = True
        except Empty:
            # никаких команд не поступило, ну и ладно
            pass
        
    def _check_events_q(self):
        """_check_events_q в цикле проверим все входящие сообщения,
        выход из цикла по условию отсутствия новых сообщений
        """

        while True:
            try:
                event: Event = self._events_q.get_nowait()
            except Empty:
                # в очереди не команд на обработку,
                # выходим из цикла проверки
                break
            if not isinstance(event, Event):
                # событие неправильного типа, пропускаем
                continue

            self._log_message(LOG_DEBUG, f"получен запрос {event}")

            if event.operation == 'log_event':
                self._log_message(LOG_INFO, "логируем ивент")
                self._log_event(event=event.parameters)

    def run(self):
        self._log_message(LOG_INFO, "старт блока логгера")

        while self._quit is False:
            sleep(self._recalc_interval_sec)
            self._check_events_q()
            self._check_control_q()
