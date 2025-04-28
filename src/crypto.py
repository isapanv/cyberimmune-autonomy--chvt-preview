import json
import base64
import datetime
from typing import Any
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.exceptions import InvalidSignature

from src.event_types import Event

def generate_rsa_keys():
    """Генерирует пару RSA-ключей (открытый и закрытый)."""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )

    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    ).decode()

    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode()

    return private_pem, public_pem

def serialize(obj: Any) -> bytes:
    """
    Универсальная сериализация объекта в детерминированную JSON-байтовую строку.
    Поддерживает любые типы в детерминированном формате.
    """
    def default_serializer(o):
        """Рекурсивно преобразует объект в JSON-сериализуемую структуру."""
        if isinstance(o, (int, float, str, bool, type(None))):
            return o
        elif isinstance(o, bytes):
            return {'__bytes__': base64.b64encode(o).decode('utf-8')}
        elif isinstance(o, (list, tuple)):
            return [default_serializer(item) for item in o]
        elif isinstance(o, dict):
            return {k: default_serializer(v) for k, v in sorted(o.items())}
        elif isinstance(o, (set, frozenset)):
            sorted_elements = sorted(o, key=lambda x: json.dumps(default_serializer(x), sort_keys=True))
            return [default_serializer(e) for e in sorted_elements]
        elif isinstance(o, datetime.datetime):
            return {'__datetime__': o.isoformat()}
        elif isinstance(o, datetime.date):
            return {'__date__': o.isoformat()}
        elif isinstance(o, datetime.time):
            return {'__time__': o.isoformat()}
        elif isinstance(o, datetime.timedelta):
            return {'__timedelta__': o.total_seconds()}
        else:
            state = {}
            if hasattr(o, '__dict__'):
                state.update(o.__dict__)
            
            slots = []
            for cls in type(o).__mro__:
                slots.extend(getattr(cls, '__slots__', []))
            seen = set()
            unique_slots = [s for s in slots if s not in seen and not seen.add(s)]
            for slot in unique_slots:
                if hasattr(o, slot):
                    state[slot] = getattr(o, slot)
            
            for attr in dir(o):
                if not callable(getattr(o, attr)) and not attr.startswith('__'):
                    if attr not in state:
                        state[attr] = getattr(o, attr)
            
            serialized_state = {k: default_serializer(v) for k, v in sorted(state.items())}
            return {
                '__class__': f"{o.__class__.__module__}.{o.__class__.__name__}",
                '__state__': serialized_state
            }

    return json.dumps(default_serializer(obj), sort_keys=True).encode('utf-8')

def create_signature(obj: Any, private_key_pem: str) -> str:
    """
    Создает цифровую подпись объекта с использованием RSA.
    - obj: объект, который нужно подписать.
    - private_key_pem: закрытый ключ в формате PEM.
    - Возвращает подпись в base64.
    """
    private_key = serialization.load_pem_private_key(private_key_pem.encode(), password=None)
    serialized_data = serialize(obj)

    signature = private_key.sign(
        serialized_data,
        padding.PKCS1v15(),
        hashes.SHA256()
    )

    return base64.b64encode(signature).decode()

def verify_signature(obj: Any, signature: str, public_key_pem: str) -> bool:
    """
    Проверяет цифровую подпись объекта с использованием RSA.
    - obj: проверяемый объект.
    - signature: цифровая подпись (base64-encoded).
    - public_key_pem: открытый ключ в формате PEM.
    - Возвращает True, если подпись верна, иначе False.
    """
    public_key = serialization.load_pem_public_key(public_key_pem.encode())
    serialized_data = serialize(obj)

    try:
        public_key.verify(
            base64.b64decode(signature),
            serialized_data,
            padding.PKCS1v15(),
            hashes.SHA256()
        )
        return True
    except InvalidSignature:
        return False

def verify_event_signature(event: Event, public_key_pem: str) -> bool:
    event_without_signature = Event(
        source=event.source,
        destination=event.destination,
        operation=event.operation,
        parameters=event.parameters,
        extra_parameters=event.extra_parameters,
    )
    return verify_signature(event_without_signature, event.signature, public_key_pem);
