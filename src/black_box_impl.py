import json
from multiprocessing import Queue
from pathlib import Path
from src.black_box import BaseBlackBox
from src.event_types import Event
from src.queues_dir import QueuesDirectory
from src.crypto import verify_signature, verify_event_signature, serialize

class BlackBox(BaseBlackBox):
    """Реализация черного ящика для безопасного хранения событий"""
    def __init__(self, queues_dir: QueuesDirectory, storage_path: str = "blackbox.log", public_key: str = None, private_key: str = None):
        super().__init__()
        self.storage_path = Path(storage_path)
        self.public_key = public_key
        self._queues_dir = queues_dir

        # создаём очередь для сообщений на обработку
        self._events_q = Queue()
        self._events_q_name = self.events_q_name
        self._queues_dir.register(
            queue=self._events_q, name=self._events_q_name)

        # Очищаем файл при инициализации
        with open(self.storage_path, 'w') as f:
            f.write("")

    def _log_event(self, event: Event) -> bool:
        """Логирует событие после проверки подписи

        Args:
            event: событие для логирования
            signature: цифровая подпись события

        Returns:
            bool: True если подпись верна и событие записано, иначе False
        """
        if not self.public_key:
            raise ValueError("Public key is not set")

        # Проверяем подпись
        is_valid = verify_event_signature(event, self.public_key)

        log_entry = {
            'event': event,
            'valid': is_valid
        }

        if not is_valid:
            log_entry['error'] = 'Invalid signature'

        with open(self.storage_path, 'a') as f:
            f.write(serialize(log_entry).decode("utf-8") + "\n")

        return is_valid
