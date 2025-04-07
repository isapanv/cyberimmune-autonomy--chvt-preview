""" модуль монитора безопасности """
from abc import abstractmethod
from multiprocessing import Queue, Process
from queue import Empty
from typing import Optional
from src.blackbox import BaseBlackBox
from time import sleep

from src.config import LOG_ERROR, SECURITY_MONITOR_QUEUE_NAME,\
    CRITICALITY_STR, DEFAULT_LOG_LEVEL, \
    LOG_DEBUG, LOG_INFO
from src.queues_dir import QueuesDirectory
from src.event_types import Event, ControlEvent


class BaseSecurityMonitor(Process):
    """ класс монитора безопасности """
    log_prefix = "[SECURITY]"
    event_source_name = SECURITY_MONITOR_QUEUE_NAME
    events_q_name = event_source_name
    log_level = DEFAULT_LOG_LEVEL

    def __init__(self, queues_dir: QueuesDirectory, public_key: str = None, blackbox: BaseBlackBox = None):
        # вызываем конструктор базового класса
        super().__init__()
        self._blackbox = blackbox
        self._queues_dir = queues_dir
        self._public_key = public_key
        # создаём очередь для сообщений на обработку
        self._events_q = Queue()
        self._events_q_name = self.event_source_name

        # регистрируем очередь в каталоге
        self._queues_dir.register(
            queue=self._events_q, name=self._events_q_name)

        # инициализируем интервал обновления
        self._recalc_interval_sec = 0.1

        self._quit = False
        # очередь управляющих команд (например, для остановки работы модуля)
        self._control_q = Queue()

        self._security_policies = {}

        self._log_message(LOG_INFO, "создан монитор безопасности")

    def _log_message(self, criticality: int, message: str):
        """_log_message печатает сообщение заданного уровня критичности

        Args:
            criticality (int): уровень критичности
            message (str): текст сообщения
        """
        if criticality <= self.log_level:
            print(f"[{CRITICALITY_STR[criticality]}]{self.log_prefix} {message}")

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

            if self._check_event(event):
                self._proceed(event)

    @abstractmethod
    def _check_event(self, event: Event):
        """ проверка события на допустимость политиками безопасности """
        # Convert legacy SITL format if needed
        if hasattr(event, 'extra_parameters'):
            event.parameters = {
                **getattr(event, 'parameters', {}),
                **event.extra_parameters
            }
            
        # Rest of your existing verification code
        if not hasattr(event, 'signature'):
            self._log_message(LOG_ERROR, "Event missing signature")
            return False

    def _proceed(self, event: Event):
        """ отправить проверенное событие конечному получателю """
        if self._blackbox and hasattr(event, 'signature'):
            # Логируем событие в черный ящик
            self._blackbox._log_event(event, event.signature)
        
        destination_q = self._queues_dir.get_queue(event.destination)
        
        if destination_q is None:
            self._log_message(
                LOG_ERROR, f"ошибка обработки запроса {event}, получатель не найден")
        else:
            destination_q.put(event)
            self._log_message(
                LOG_DEBUG, f"запрос отправлен получателю {event}")

    def stop(self):
        """stop запрос остановки работы блока
        """
        self._control_q.put(ControlEvent(operation='stop'))

    def run(self):
        self._log_message(LOG_INFO, "старт блока грузового отсека")

        while self._quit is False:
            sleep(self._recalc_interval_sec)
            self._check_events_q()
            self._check_control_q()
