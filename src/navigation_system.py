""" модуль системы навигации """
from multiprocessing import Queue, Process
from abc import abstractmethod
from queue import Empty
from time import sleep

from geopy import Point

from src.config import CRITICALITY_STR, \
    LOG_DEBUG, LOG_ERROR, LOG_INFO, NAVIGATION_QUEUE_NAME, DEFAULT_LOG_LEVEL
from src.queues_dir import QueuesDirectory
from src.event_types import Event, ControlEvent


class BaseNavigationSystem(Process):
    """BaseNavigationSystem базовый класс блока навигации  """

    log_prefix = "[NAVIGATION]"
    event_source_name = NAVIGATION_QUEUE_NAME
    events_q_name = event_source_name

    def __init__(self, queues_dir: QueuesDirectory, log_level=DEFAULT_LOG_LEVEL, public_key: str = None, private_key: str = None):
        # вызываем конструктор базового класса
        super().__init__()
        
        self.public_key = public_key
        self.private_key = private_key

        self._queues_dir = queues_dir

        # создаём очередь для сообщений на обработку
        self._events_q = Queue()
        self._events_q_name = self.events_q_name
        self._queues_dir.register(
            queue=self._events_q, name=self._events_q_name)

        self._quit = False
        # очередь управляющих команд (например, для остановки работы модуля)
        self._control_q = Queue()

        # инициализируем интервал обновления
        self._recalc_interval_sec = 0.5

        self.log_level = log_level
        self._position = None

        self._log_message(LOG_INFO, "создан компонент навигации")

    def _log_message(self, criticality: int, message: str):
        """_log_message печатает сообщение заданного уровня критичности

        Args:
            criticality (int): уровень критичности
            message (str): текст сообщения
        """
        if criticality <= self.log_level:
            print(f"[{CRITICALITY_STR[criticality]}]{self.log_prefix} {message}")

    def stop(self):
        """ запрос остановки работы """
        self._control_q.put(ControlEvent(operation='stop'))

    # проверка наличия новых управляющих команд
    def _check_control_q(self):
        try:
            request: ControlEvent = self._control_q.get_nowait()
            self._log_message(LOG_DEBUG, f"проверяем запрос {request}")
            if isinstance(request, ControlEvent) and request.operation == 'stop':
                # поступил запрос на остановку монитора, поднимаем "красный флаг"
                self._quit = True
        except Empty:
            # никаких команд не поступило, ну и ладно
            pass

    def _request_coordinates(self):
        try:
            request = Event(source=self.event_source_name,
                            destination="sitl",
                            operation="post_position",
                            parameters=None
                            )
            sitl_q: Queue = self._queues_dir.get_queue("sitl")
            sitl_q.put(request)
        except Exception as e:
            self._log_message(LOG_ERROR, f"ошибка запроса координат: {e}")

    def _read_coordinates(self):
        try:
            event: Event = self._events_q.get_nowait()
            if isinstance(event, Event) and event.operation == 'position_update':
                self._position: Point = event.parameters
                self._log_message(
                    LOG_DEBUG, f"получены новые координаты {self._position.longitude}, " +
                    f"{self._position.latitude}")
                self._send_position_to_consumers()
        except Empty:
            # никаких команд не поступило, ну и ладно
            pass
        except Exception as e:
            self._log_message(LOG_ERROR, f"ошибка получения координат: {e}")

    @abstractmethod
    def _send_position_to_consumers(self):
        pass

    def run(self):
        self._log_message(LOG_INFO, "старт навигации")

        while self._quit is False:
            sleep(self._recalc_interval_sec)
            try:
                self._request_coordinates()
                self._read_coordinates()
            except Exception as e:
                self._log_message(
                    LOG_ERROR, f"ошибка обновления координат: {e}")

            self._check_control_q()
