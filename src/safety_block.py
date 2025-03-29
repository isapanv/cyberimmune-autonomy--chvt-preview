""" модуль для реализации базового блока "Ограничитель"
"""

from abc import abstractmethod
from queue import Empty
from multiprocessing import Queue, Process
from time import sleep
from typing import Optional
from geopy import Point as GeoPoint

from config import CRITICALITY_STR, DEFAULT_LOG_LEVEL, SAFETY_BLOCK_QUEUE_NAME, \
    LOG_ERROR, LOG_DEBUG, LOG_INFO
from mission_type import Mission
from queues_dir import QueuesDirectory
from event_types import Event, ControlEvent
from route import Route
from crypto_module import *

class BaseSafetyBlock(Process):
    """SafetyBlock класс для реализации блока "Ограничитель"""
    log_prefix = "[SAFETY]"
    event_source_name = SAFETY_BLOCK_QUEUE_NAME
    events_q_name = event_source_name

    def __init__(self, queues_dir: QueuesDirectory, log_level=DEFAULT_LOG_LEVEL):
        super().__init__()  # Process.__init__ takes no arguments
        self._queues_dir = queues_dir
        self.log_level = log_level

        # создаём очередь для сообщений на обработку
        self._events_q = Queue()
        self._events_q_name = self.event_source_name

        # регистрируем очередь в каталоге
        self._queues_dir.register(
            queue=self._events_q, name=self._events_q_name)

        # инициализируем интервал обновления
        self._tolerance_meters = 5
        self._recalc_interval_sec = 0.5

        self._quit = False
        # очередь управляющих команд (например, для остановки работы модуля)
        self._control_q = Queue()

        self._speed: int = 0
        self._direction: float = 0.0
        self._mission : Optional[Mission] = None
        self._position : Optional[GeoPoint] = None

        self.log_level = log_level
        self._log_message(LOG_INFO, "создан ограничитель")
        self._enabled_handlers = {
            "set_mission": self._set_mission,
            "set_speed": self._set_new_speed,
            "set_direction": self._set_new_direction,
            "position_update": self._set_new_position,
            "lock_cargo": self._lock_cargo,
            "release_cargo": self._release_cargo
        }
        self._route: Optional[Route] = None


    
    def _log_message(self, criticality: int, message: str):
        """_log_message печатает сообщение заданного уровня критичности

        Args:
            criticality (int): уровень критичности
            message (str): текст сообщения
        """
        if criticality <= self.log_level:
            print(f"[{CRITICALITY_STR[criticality]}]{self.log_prefix} {message}")

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


    def _set_mission(self, mission: Mission):
        """ установка нового маршрутного задания """
        self._mission = mission
        self._route = Route(points=self._mission.waypoints,
                            speed_limits=self._mission.speed_limits)

    @abstractmethod
    def _set_new_direction(self, direction: float):
        """ установка нового направления перемещения """

    @abstractmethod
    def _set_new_speed(self, speed: float):
        """ установка новой скорости """


    @abstractmethod
    def _lock_cargo(self, _):
        """ блокировка грузового отсека """

    @abstractmethod
    def _release_cargo(self, _):
        """ разблокировка грузового отсека """


    def _set_new_position(self, position: GeoPoint):
        """ установка новых координат """
        self._log_message(LOG_DEBUG, f"установка местоположения {position}")

        self._position = position

        distance_to_next_wp = self._route.calculate_remaining_distance_to_next_point(
            self._position)

        if distance_to_next_wp <= self._tolerance_meters:
            self._route.move_to_next_point()
            if self._route.route_finished:
                self._log_message(LOG_INFO, "маршрут пройден")
            else:
                self._log_message(LOG_INFO, "сегмент пройден")

    def _verify_event(self, event: Event) -> bool:
        """Verify the event signature"""
        if not hasattr(event, 'signature'):
            self._log_message(LOG_ERROR, f"Event {event} has no signature")
            return False
            
        verification_data = {
            "source": event.source,
            "destination": event.destination,
            "operation": event.operation,
            "parameters": event.parameters
        }
        
        try:
            return verify_signature(verification_data, event.signature, self.public_key)
        except Exception as e:
            self._log_message(LOG_ERROR, f"Signature verification failed: {e}")
            return False

    def _check_events_q(self):
        """Check incoming events with signature verification"""
        while True:
            try:
                event: Event = self._events_q.get_nowait()
            except Empty:
                break
                
            if not isinstance(event, Event):
                continue

            self._log_message(LOG_DEBUG, f"Received request {event}")

            # Verify signature before processing
            if not self._verify_event(event):
                self._log_message(LOG_ERROR, f"Invalid signature for event {event}")
                continue

            if event.operation in self._enabled_handlers:
                handler = self._enabled_handlers[event.operation]
                handler(event.parameters)
            else:
                self._log_message(LOG_ERROR, f"Unknown operation: {event}")


    @abstractmethod
    def _send_speed_to_consumers(self):
        self._log_message(LOG_DEBUG, "Sending speed to consumers")
        servos_q_name = SERVOS_QUEUE_NAME
        servos_q = self._queues_dir.get_queue(servos_q_name)

        event = Event(
            source=self.event_source_name,
            destination=servos_q_name,
            operation="set_speed",
            parameters=self._speed
        )
        
        # Use dictionary for signing
        signed_data = {
            "source": self.event_source_name,
            "destination": servos_q_name,
            "operation": "set_speed",
            "parameters": self._speed
        }
        event.signature = create_signature(signed_data, self.private_key)
        servos_q.put(event)


    @abstractmethod
    def _send_direction_to_consumers(self):
        self._log_message(LOG_DEBUG, "Sending direction to consumers")
        servos_q_name = SERVOS_QUEUE_NAME
        servos_q = self._queues_dir.get_queue(servos_q_name)

        event = Event(
            source=self.event_source_name,
            destination=servos_q_name,
            operation="set_direction",
            parameters=self._direction
        )
        
        signed_data = {
            "source": self.event_source_name,
            "destination": servos_q_name,
            "operation": "set_direction",
            "parameters": self._direction
        }
        event.signature = create_signature(signed_data, self.private_key)
        servos_q.put(event)

    @abstractmethod
    def _send_lock_cargo_to_consumers(self):
        self._log_message(LOG_DEBUG, "Sending lock command to CargoBay")
        event = Event(
            source=self.event_source_name,
            destination=CARGO_BAY_QUEUE_NAME,
            operation="lock_cargo",
            parameters=None
        )
        
        signed_data = {
            "source": self.event_source_name,
            "destination": CARGO_BAY_QUEUE_NAME,
            "operation": "lock_cargo",
            "parameters": None
        }
        event.signature = create_signature(signed_data, self.private_key)
        self._queues_dir.get_queue(CARGO_BAY_QUEUE_NAME).put(event)

    @abstractmethod
    def _send_release_cargo_to_consumers(self):
        self._log_message(LOG_DEBUG, "Sending release command to CargoBay")
        event = Event(
            source=self.event_source_name,
            destination=CARGO_BAY_QUEUE_NAME,
            operation="release_cargo",
            parameters=None
        )
        
        signed_data = {
            "source": self.event_source_name,
            "destination": CARGO_BAY_QUEUE_NAME,
            "operation": "release_cargo",
            "parameters": None
        }
        event.signature = create_signature(signed_data, self.private_key)
        self._queues_dir.get_queue(CARGO_BAY_QUEUE_NAME).put(event)

    def stop(self):
        """ метод для остановки работы блока,
        отправляет управляющий запрос на остановку во внутреннюю очередь"""
        self._control_q.put(ControlEvent(operation='stop'))

    def run(self):
        """ вызывается при запуске процесса """
        self._log_message(LOG_INFO, "старт ограничителя")

        while self._quit is False:
            sleep(self._recalc_interval_sec)
            try:
                self._check_events_q()
                self._check_control_q()
            except Exception as e:
                self._log_message(LOG_ERROR, f"ошибка обработки команд: {e}")