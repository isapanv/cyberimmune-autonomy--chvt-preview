""" модуль для расчёт управляющих команд
(изменения направления и скорости движения для заданного маршрута и с учётом ограничений)
"""

from abc import abstractmethod
import datetime
from multiprocessing import Queue, Process
from queue import Empty
import math
from time import sleep
from typing import Optional

from geopy import Point as GeoPoint

from src.queues_dir import QueuesDirectory
from src.mission_type import Mission
from src.event_types import Event, ControlEvent
from src.config import CONTROL_SYSTEM_QUEUE_NAME, \
    CRITICALITY_STR, DEFAULT_LOG_LEVEL, LOG_DEBUG, LOG_ERROR, LOG_INFO
from src.route import Route


class BaseControlSystem(Process):
    """ базовый класс для блока управления """
    log_prefix = "[CONTROL]"
    event_source_name = CONTROL_SYSTEM_QUEUE_NAME
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
        self._tolerance_meters = 5  # радиус достижения путевой точки

        self._queues_dir.register(
            queue=self._events_q, name=self._events_q_name)

        self._quit = False
        # очередь управляющих команд (например, для остановки работы модуля)
        self._control_q = Queue()

        # инициализируем интервал обновления
        self._recalc_interval_sec = 0.1

        self.log_level = log_level
        self._position = None
        self._route: Optional[Route] = None
        self._mission: Optional[Mission] = None
        self._last_speed_limit = -1
        self._speed = 0
        self._direction_grad = 0.0
        self._surprises_enabled = False

        self._log_message(LOG_INFO, "создана система управления")

    def _log_message(self, criticality: int, message: str):
        """_log_message печатает сообщение заданного уровня критичности

        Args:
            criticality (int): уровень критичности
            message (str): текст сообщения
        """
        if criticality <= self.log_level:
            print(f"[{CRITICALITY_STR[criticality]}]{self.log_prefix} {message}")

    def _set_speed(self, speed_kmh: float):
        """
        Устанавливает текущую скорость перемещения.

        Args:
        speed_kmh (float): Скорость в км/ч.

        Raises:
            ValueError: Если скорость отрицательная.
        """
        if speed_kmh < 0:
            raise ValueError("Скорость не может быть отрицательной.")

        self._speed = speed_kmh * 1000 / 3600  # Преобразуем в м/с

    def _set_direction(self, direction_grad: float):
        """
        Устанавливает текущую скорость перемещения.

        Args:
        speed_kmh (float): Скорость в км/ч.

        Raises:
            ValueError: Если скорость отрицательная.
        """
        if direction_grad < 0:
            raise ValueError("направление не может быть отрицательным.")

        self._direction_grad = direction_grad

    def _set_mission(self, mission: Mission):
        self._mission = mission
        self._route = Route(points=self._mission.waypoints,
                            speed_limits=self._mission.speed_limits)

        self._log_message(
            LOG_DEBUG, f"получено маршрутное задание: {self._mission}")
        self._log_message(
            LOG_INFO, "установлена новая задача, начинаем следовать по маршруту, " +
            f"текущее время {datetime.datetime.now().time()}")

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

    def _calculate_bearing(self, start: GeoPoint, end: GeoPoint) -> float:
        """_calculate_bearing возвращает направление перемещения

        Args:
            start (GeoPoint): откуда
            end (GeoPoint): куда

        Returns:
            float: направление в градусах 0..360
        """
        delta_longitude = end.longitude - start.longitude
        x = math.sin(math.radians(delta_longitude)) * \
            math.cos(math.radians(end.latitude))
        y = math.cos(math.radians(start.latitude)) * math.sin(math.radians(end.latitude)) - \
            math.sin(math.radians(start.latitude)) * math.cos(math.radians(end.latitude)) * \
            math.cos(math.radians(delta_longitude))

        initial_bearing_rad = math.atan2(x, y)

        # Преобразуем радианы в градусы
        initial_bearing_deg = math.degrees(initial_bearing_rad)

        # Нормализуем значение в диапазоне [0, 360]
        compass_bearing = (initial_bearing_deg + 360) % 360

        return compass_bearing

    def _calculate_current_bearing(self) -> float:
        """_calculate_bearing пересчёт направления с учётом текущих координат (self._position)
        и маршрутного задания (self._mission)

        Returns:
            float: направление движения в градусах
        """
        if self._route.route_finished:
            return 0.0
        pos: GeoPoint = self._position
        dst: GeoPoint = self._route.next_point()
        bearing = self._calculate_bearing(pos, dst)
        if self._surprises_enabled and (self._route.current_index == 1):
            bearing += 180
            bearing = bearing % 360
        self._log_message(LOG_DEBUG, f"новое направление {bearing}")
        return bearing

    @abstractmethod
    def _send_speed_and_direction_to_consumers(self, speed: float, direction: float):
        pass

    @abstractmethod
    def _release_cargo(self):
        pass

    @abstractmethod
    def _lock_cargo(self):
        pass

    def enable_surprises(self):
        """ активация киберпрепятствий """
        self._log_message(LOG_DEBUG, "активация киберпрепятствий")
        self._surprises_enabled = True

    def _recalc_control(self):

        new_speed = 0
        new_direction = 0

        if self._route.route_finished:
            # уже на месте, стоим, ждём
            return
        self._log_message(LOG_DEBUG, "пересчитываем управление")

        distance_to_next_wp = self._route.calculate_remaining_distance_to_next_point(
            self._position)

        if distance_to_next_wp <= self._tolerance_meters:
            self._route.move_to_next_point()

            if self._surprises_enabled and (self._route.current_index == 3):
                self._release_cargo()

            if self._route.route_finished:
                self._log_message(
                    LOG_INFO, "маршрут пройден, " +
                    f"текущее время {datetime.datetime.now().time()}")
                # оставить груз
                self._release_cargo()
            else:
                self._log_message(LOG_INFO, "сегмент пройден")

        new_speed = self._route.calculate_speed()

        if self._surprises_enabled and (self._route.current_index == 2):
            new_speed += 100

        if int(self._speed) != int(new_speed/3.6):
            self._log_message(
                LOG_INFO, f"новая скорость {new_speed} (была {int(self._speed*3.6)})")

        self._set_speed(new_speed)

        new_direction = self._calculate_current_bearing()
        if int(self._direction_grad) != int(new_direction):
            self._log_message(
                LOG_INFO, f"новое направление {int(new_direction)} " +
                f"(было {int(self._direction_grad)})")
        self._set_direction(new_direction)

        self._log_message(
            LOG_DEBUG, f"до следующей точки {int(distance_to_next_wp)} м\t" +
            f"скорость {int(new_speed)}км/ч\tнаправление {int(new_direction)} град."
        )

        self._send_speed_and_direction_to_consumers(new_speed, new_direction)

    def _check_events_q(self):
        """_check_events_q
        проверяет входящие события до их полного исчерпания
        """

        while True:
            try:
                event: Event = self._events_q.get_nowait()
                if not isinstance(event, Event):
                    return
                if event.operation == 'set_mission':
                    self._set_mission(event.parameters)
                    self._lock_cargo()
                elif event.operation == "position_update":
                    self._position = event.parameters
                    if self._route is not None:
                        # пересчитаем направление движения и скорость, если уже есть маршрут
                        self._recalc_control()
            except Empty:
                # никаких команд не поступило, ну и ладно
                break

    def stop(self):
        """ запрос остановки работы """
        self._control_q.put(ControlEvent(operation='stop'))

    def run(self):
        self._log_message(LOG_INFO, "старт системы управления")

        while self._quit is False:
            sleep(self._recalc_interval_sec)
            try:
                self._check_events_q()
                self._check_control_q()
            except Exception as e:
                self._log_message(LOG_ERROR, f"ошибка системы управления: {e}")
