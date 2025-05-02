import pytest
from multiprocessing import Queue
from pathlib import Path
import sys
from os.path import abspath, dirname
import os
import json
import time
import shutil

sys.path.insert(0, abspath(dirname(__file__) + '/..'))

from src.black_box_impl import BlackBox
from src.crypto import generate_rsa_keys, create_signature
from src.event_types import Event
from src.queues_dir import QueuesDirectory

# Фикстура для генерации пары RSA-ключей
@pytest.fixture
def rsa_keys():
    return generate_rsa_keys()

# Фикстура для временного файла лога
@pytest.fixture
def temp_log_file(tmp_path):
    return tmp_path / "blackbox_test.log"

# Фикстура BlackBox с настроенной очередью
@pytest.fixture
def blackbox(tmp_path, rsa_keys):
    private_key, public_key = rsa_keys
    queues_dir = QueuesDirectory()
    bb = BlackBox(queues_dir=queues_dir, storage_path=tmp_path, public_key=public_key)
    bb.queue = queues_dir.get_queue(bb.events_q_name)  # Упрощаем доступ к очереди
    return bb

# Фикстура корректного события с подписью
@pytest.fixture
def valid_event(rsa_keys):
    private_key, public_key = rsa_keys
    event = Event(
        source="validator",
        destination="verifier",
        operation="log_event",
        parameters={"value": 42},
        extra_parameters={},
    )
    event.signature = create_signature(event, private_key)
    return event

# Проверяем логирование события с корректной подписью
def test_log_event_valid_signature(rsa_keys, temp_log_file):
    private_key, public_key = rsa_keys
    queues_dir = QueuesDirectory()
    bb = BlackBox(queues_dir=queues_dir, storage_path=temp_log_file, public_key=public_key)

    event = Event(
        source="test_src",
        destination="test_dest",
        operation="test_op",
        parameters={"foo": "bar"},
        extra_parameters={},
    )
    event.signature = create_signature(event, private_key)

    result = bb._log_event(event)
    assert result is True

    with open(temp_log_file, 'r') as f:
        logged = f.read()
        assert "Invalid signature" not in logged
        assert '"valid": true' in logged

# Проверка логирования с некорректной подписью
def test_log_event_invalid_signature(rsa_keys, temp_log_file):
    private_key, public_key = rsa_keys
    queues_dir = QueuesDirectory()
    bb = BlackBox(queues_dir=queues_dir, storage_path=temp_log_file, public_key=public_key)

    event = Event(
        source="hacker",
        destination="target",
        operation="exploit",
        parameters={"virus": "🦠"},
        extra_parameters={},
    )
    # Подписываем не тот объект, чтобы подпись не соответствовала событию
    wrong_data = {"not": "this event"}
    event.signature = create_signature(wrong_data, private_key)

    result = bb._log_event(event)
    assert result is False

    with open(temp_log_file, 'r') as f:
        logged = f.read()
        assert '"valid": false' in logged
        assert 'Invalid signature' in logged

# Проверка поведения при отсутствии открытого ключа
def test_log_event_without_public_key_raises(temp_log_file, rsa_keys):
    private_key, _ = rsa_keys
    queues_dir = QueuesDirectory()
    bb = BlackBox(queues_dir=queues_dir, storage_path=temp_log_file, public_key=None)

    event = Event(
        source="mystery",
        destination="nowhere",
        operation="ghost",
        parameters={"boo": True},
        extra_parameters={},
    )
    event.signature = create_signature(event, private_key)

    # Ожидаем ошибку из-за отсутствия публичного ключа
    with pytest.raises(ValueError, match="Public key is not set"):
        bb._log_event(event)

# Логирование нескольких событий подряд
def test_multiple_events_logged(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    queues_dir = QueuesDirectory()
    bb = BlackBox(queues_dir=queues_dir, storage_path=temp_log_file, public_key=public_key)

    for i in range(3):
        event = Event(
            source=f"src{i}",
            destination=f"dest{i}",
            operation="log_event",
            parameters={"id": i},
            extra_parameters={},
        )
        event.signature = create_signature(event, private_key)
        bb._log_event(event)

    with open(temp_log_file, "r") as f:
        lines = f.readlines()

    assert len(lines) == 3
    for line in lines:
        log_entry = json.loads(line)
        assert log_entry["valid"] is True

# Проверка повреждённой подписи
def test_blackbox_rejects_corrupted_signature(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    queues_dir = QueuesDirectory()
    bb = BlackBox(queues_dir=queues_dir, storage_path=temp_log_file, public_key=public_key)

    event = Event(
        source="goodguy",
        destination="server",
        operation="upload",
        parameters={"file": "safe.txt"},
        extra_parameters={}
    )
    event.signature = create_signature(event, private_key)

    # Подделываем подпись вручную
    event.signature = 'aGVsbG8gd29ybGQ='  # base64 от "hello world"

    result = bb._log_event(event)
    assert result is False

    with open(temp_log_file, "r") as f:
        lines = f.readlines()

    assert len(lines) == 1
    log_entry = json.loads(lines[0])
    assert log_entry["valid"] is False

# Утилита: ожидание появления лога
def wait_for_log(file_path, timeout=3):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with open(file_path, "r") as f:
                lines = f.readlines()
                if lines:
                    return lines
        except FileNotFoundError:
            pass
        time.sleep(0.1)
    return []

# Проверка события с отсутствующими полями
def test_log_event_missing_fields(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    queues_dir = QueuesDirectory()
    bb = BlackBox(queues_dir=queues_dir, storage_path=temp_log_file, public_key=public_key)

    event = Event(
        source=None,
        destination=None,
        operation="upload",
        parameters={},
        extra_parameters={}
    )
    event.signature = create_signature(event, private_key)

    result = bb._log_event(event)
    assert result is False

    with open(temp_log_file, "r") as f:
        logs = json.load(f)
        assert logs["valid"] is False

# Проверка повторного логирования одного и того же события
def test_log_duplicate_event(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    queues_dir = QueuesDirectory()
    bb = BlackBox(queues_dir=queues_dir, storage_path=temp_log_file, public_key=public_key)

    event = Event(
        source="repeat",
        destination="loop",
        operation="ping",
        parameters={"msg": "again"},
        extra_parameters={}
    )
    event.signature = create_signature(event, private_key)

    bb._log_event(event)
    bb._log_event(event)

    with open(temp_log_file, "r") as f:
        lines = f.readlines()

    assert len(lines) == 2  

# Проверка изменения параметров после подписания
def test_log_event_with_tampered_parameters(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)

    event = Event(
        source="sender",
        destination="receiver",
        operation="send_data",
        parameters={"data": "original"},
        extra_parameters={}
    )
    event.signature = create_signature(event, private_key)

    event.parameters["data"] = "tampered"

    result = bb._log_event(event)
    assert result is False

# Проверка события с пустыми параметрами и подписью
def test_log_event_empty_parameters_and_signature(temp_log_file, rsa_keys):
    _, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)

    event = Event(
        source="blank",
        destination="void",
        operation="noop",
        parameters={},
        extra_parameters={}
    )
    event.signature = "" 

    result = bb._log_event(event)
    assert result is False

# Проверка логирования события с большим объёмом данных
def test_log_event_large_payload(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)

    big_data = {"blob": "x" * 10_000_000} 
    event = Event(
        source="big_sender",
        destination="big_receiver",
        operation="big_upload",
        parameters=big_data,
        extra_parameters={}
    )
    event.signature = create_signature(event, private_key)

    result = bb._log_event(event)
    assert result is True

# Подпись явно None
def test_log_event_with_none_signature(temp_log_file, rsa_keys):
    _, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)

    event = Event(
        source="ghost",
        destination="specter",
        operation="haunt",
        parameters={"boo": 1},
        extra_parameters={}
    )
    event.signature = None

    result = bb._log_event(event)
    assert result is False

# Проверка поведения с дополнительными неожиданными полями
def test_log_event_with_unexpected_fields(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)

    event = Event(
        source="extra",
        destination="handler",
        operation="surprise",
        parameters={"legit": 1},
        extra_parameters={"unexpected": "oops"}  
    )
    event.signature = create_signature(event, private_key)
    result = bb._log_event(event)
    assert result is True 

# Проверка логирования при повреждённом JSON-логе
def test_log_file_with_invalid_json(tmp_path, rsa_keys):
    private_key, public_key = rsa_keys
    log_path = tmp_path / "corrupt_log.json"

    with open(log_path, "w") as f:
        f.write("this is not valid json\n")  
    bb = BlackBox(QueuesDirectory(), log_path, public_key)

    event = Event(
        source="clean",
        destination="system",
        operation="recovery",
        parameters={"fix": True},
        extra_parameters={}
    )
    event.signature = create_signature(event, private_key)

    result = bb._log_event(event)
    assert result is True

    with open(log_path) as f:
        lines = f.readlines()

    assert any("valid" in line for line in lines) 