import pytest
import json
from pathlib import Path
from src.event_types import Event
from src.blackbox import BlackBox
from your_crypto_module import generate_rsa_keys, create_signature, verify_signature

@pytest.fixture
def setup_blackbox(tmp_path):
    # Генерируем тестовые ключи
    private_key, public_key = generate_rsa_keys()
    
    # Создаем временный файл для логов
    log_file = tmp_path / "blackbox_test.log"
    
    # Создаем экземпляр BlackBox
    blackbox = BlackBox(storage_path=str(log_file), public_key_pem=public_key)
    
    yield blackbox, private_key, public_key, log_file
    
    # Очистка после теста
    blackbox.stop()

def test_blackbox_valid_event(setup_blackbox):
    blackbox, private_key, public_key, log_file = setup_blackbox
    
    # Создаем тестовое событие
    test_event = Event(operation="test", parameters={"param1": "value1"})
    
    # Создаем подпись
    signature = create_signature(test_event, private_key)
    
    # Логируем событие
    result = blackbox._log_event(test_event, signature)
    
    assert result is True, "Событие с валидной подписью должно быть записано"
    
    # Проверяем запись в файле
    with open(log_file, 'r') as f:
        lines = f.readlines()
        assert len(lines) == 1, "Должна быть одна запись в логе"
        
        entry = json.loads(lines[0])
        assert entry['valid'] is True, "Запись должна быть помечена как валидная"
        assert entry['event']['operation'] == "test", "Операция события должна сохраниться"

def test_blackbox_invalid_event(setup_blackbox):
    blackbox, private_key, public_key, log_file = setup_blackbox
    
    # Создаем тестовое событие
    test_event = Event(operation="test", parameters={"param1": "value1"})
    
    # Создаем НЕправильную подпись (используем другое событие для подписи)
    wrong_event = Event(operation="wrong", parameters={"param2": "value2"})
    signature = create_signature(wrong_event, private_key)
    
    # Логируем событие с неправильной подписью
    result = blackbox._log_event(test_event, signature)
    
    assert result is False, "Событие с невалидной подписью не должно быть принято"
    
    # Проверяем запись в файле
    with open(log_file, 'r') as f:
        lines = f.readlines()
        assert len(lines) == 1, "Должна быть одна запись в логе"
        
        entry = json.loads(lines[0])
        assert entry['valid'] is False, "Запись должна быть помечена как невалидная"
        assert 'error' in entry, "Должна быть указана ошибка"

def test_blackbox_corrupted_event(setup_blackbox):
    blackbox, private_key, public_key, log_file = setup_blackbox
    
    # Создаем тестовое событие
    test_event = Event(operation="test", parameters={"param1": "value1"})
    
    # Создаем корректную подпись
    signature = create_signature(test_event, private_key)
    
    # Изменяем событие после подписи
    test_event.operation = "hacked"
    
    # Логируем измененное событие
    result = blackbox._log_event(test_event, signature)
    
    assert result is False, "Измененное событие не должно быть принято"
    
    # Проверяем запись в файле
    with open(log_file, 'r') as f:
        lines = f.readlines()
        assert len(lines) == 1, "Должна быть одна запись в логе"
        
        entry = json.loads(lines[0])
        assert entry['valid'] is False, "Запись должна быть помечена как невалидная"
        assert entry['event']['operation'] == "hacked", "Измененная операция должна сохраниться"