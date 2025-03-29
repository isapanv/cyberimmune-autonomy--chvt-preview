import json
import base64
import hashlib
import json
from typing import Any
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.exceptions import InvalidSignature


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
    - Поддерживает списки, словари, встроенные типы и объекты с атрибутами.
    - Сортирует ключи для детерминированности.
    """
    def default_serializer(o):
        """Рекурсивно преобразует объект в JSON-сериализуемую структуру."""
        if isinstance(o, (int, float, str, bool, type(None))):
            return o
        elif isinstance(o, list):
            return [default_serializer(item) for item in o]
        elif isinstance(o, dict):
            return {key: default_serializer(value) for key, value in sorted(o.items())}
        elif hasattr(o, "__dict__"):  # Преобразуем объект в словарь
            return {key: default_serializer(value) for key, value in sorted(o.__dict__.items())}
        else:
            raise TypeError(f"Неподдерживаемый тип: {type(o)}")

    return json.dumps(default_serializer(obj), sort_keys=True).encode("utf-8")

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