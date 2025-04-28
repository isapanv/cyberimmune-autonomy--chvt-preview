"""" модуль симулятора движения """
from multiprocessing import Queue, Process
from queue import Empty
from time import sleep
from geopy import Point, distance

from src.config import CRITICALITY_STR, LOG_DEBUG, \
    LOG_ERROR, LOG_INFO, SITL_QUEUE_NAME, NAVIGATION_QUEUE_NAME, \
    SITL_TELEMETRY_QUEUE_NAME, DEFAULT_LOG_LEVEL
from src.queues_dir import QueuesDirectory
from src.event_types import Event, ControlEvent


# симулятор движения машинки
class SITL(Process):
    """ симулятор движения """
    log_prefix = "[SITL]"
    event_source_name = SITL_QUEUE_NAME
    events_q_name = event_source_name

    def __init__(
            self, queues_dir: QueuesDirectory,
            position: Point = None,
            car_id: str = "C1",
            post_telemetry: bool = False,
            log_level = DEFAULT_LOG_LEVEL
    ):
        # вызываем конструктор базового класса
        super().__init__()

        # задаём начальное положение машинки на плоскости
        if position is None:
            position = Point(0.0, 0.0)

        self._queues_dir = queues_dir
        # создаём очередь для сообщений на обработку
        self._events_q = Queue()
        self._events_q_name = SITL.events_q_name
        queues_dir.register(queue=self._events_q, name=self._events_q_name)

        self._position = position
        self._car_id = car_id

        # инициализируем скорость и направление движения
        self._speed_kmph = 0  # скорость в километрах в час
        self._bearing = 0     # направление движения в радианах

        self._quit = False
        # очередь управляющих команд (например, для остановки симуляции)
        self._control_q = Queue()

        self._post_telemetry_enabled = post_telemetry

        # инициализируем интервал обновления
        self._recalc_interval_sec = 0.1
        self.log_level = log_level
        self._log_message(LOG_INFO, f"симулятор создан, ID {self._car_id}")

    def _log_message(self, criticality: int, message: str):
        """_log_message печатает сообщение заданного уровня критичности

        Args:
            criticality (int): уровень критичности
            message (str): текст сообщения
        """
        if criticality <= self.log_level:
            print(f"[{CRITICALITY_STR[criticality]}]{self.log_prefix} {message}")

    def set_speed(self, speed: float = 0.0):
        """set_speed установка нового значения скорости

        Args:
            speed (float, optional): новое значение скорости. Defaults to 0.0.
        """
        self._log_message(
            LOG_DEBUG, f"устанавливаем новую скорость движения {speed}")
        self._speed_kmph = speed

    def set_direction(self, bearing: float = 0.0):
        """set_direction установка нового значения

        Args:
            bearing (float, optional): новое значение направления. Defaults to 0.0.
        """
        self._log_message(
            LOG_DEBUG, f"устанавливаем новое направление движения {int(bearing)}")
        self._bearing = bearing

    def get_coordinates(self):
        """get_coordinates отправляет запрос на текущие координаты
        """
        self._control_q.put(ControlEvent(operation='post_position'))

    def car_id(self) -> str:
        """car_id выдаёт идентификатор машинки

        Returns:
            car_id(str): строковый идентификатор
        """
        return self._car_id

    def stop(self):
        """stop отправка сигнала на остановку симуляции
        """
        self._control_q.put(ControlEvent(operation='stop'))

    # проверка наличия новых управляющих команд
    def _check_control_q(self):
        try:
            request: ControlEvent = self._control_q.get_nowait()
            self._log_message(
                LOG_DEBUG, f"{self.log_prefix} проверяем запрос {request}")
            if not isinstance(request, ControlEvent):
                return
            if request.operation == 'stop':
                # поступил запрос на остановку монитора, поднимаем "красный флаг"
                self._quit = True
        except Empty:
            # никаких команд не поступило, ну и ладно
            pass

    def _post_telemetry(self):
        event = Event(source=SITL.event_source_name,
                      destination=SITL_TELEMETRY_QUEUE_NAME,
                      operation="post_telemetry",
                      parameters=self._position,
                      extra_parameters={
                          "bearing": self._bearing, "speed": self._speed_kmph}
                      )
        telemetry_gateway_q = self._queues_dir.get_queue(
            SITL_TELEMETRY_QUEUE_NAME)
        try:
            telemetry_gateway_q.put(event)
        except Exception as e:
            self._log_message(
                LOG_ERROR, f"{self.log_prefix} ошибка отправки телеметрии: {e}")

    def _check_events_q(self):
        while True:
            try:
                event: Event = self._events_q.get_nowait()
                # print(f"{self.log_prefix} обрабатываем событие: {event}")
                if not isinstance(event, Event):
                    return
                if event.operation == 'post_position':
                    try:
                        nav_q = self._queues_dir.get_queue('navigation')
                        nav_q.put(Event(source=SITL.event_source_name,
                                        destination=NAVIGATION_QUEUE_NAME,
                                        operation="position_update",
                                        parameters=self._position)
                                  )
                    except Exception as e:
                        self._log_message(
                            LOG_ERROR, f"{self.log_prefix} ошибка отправки координат: {e}")
                    if self._post_telemetry_enabled:
                        self._post_telemetry()
                elif event.operation == 'set_speed':
                    self.set_speed(float(event.parameters))
                elif event.operation == 'set_direction':
                    self.set_direction(float(event.parameters))
            except Empty:
                # все входящие события обработаны
                break

    def _recalc(self):
        distance_km = self._recalc_interval_sec / 3600 * self._speed_kmph

        new_pos = distance.distance(kilometers=distance_km).destination(
            point=self._position, bearing=self._bearing)
        self._position = new_pos
        # print(f"{self.log_prefix} пересчёт положения завершён:
        # долгота, широта {self._position.longitude}, {self._position.latitude}")

    def run(self):
        self._log_message(LOG_INFO, f"{self.log_prefix} старт симуляции")

        while self._quit is False:

            if self._speed_kmph != 0:
                self._recalc()

            self._check_events_q()
            self._check_control_q()

            sleep(self._recalc_interval_sec)
