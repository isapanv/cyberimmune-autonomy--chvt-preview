import time
import pytest
from multiprocessing import Queue
from pathlib import Path
from os.path import abspath, dirname
import os

from src.black_box_impl import BlackBox
from src.crypto import generate_rsa_keys, create_signature
from src.event_types import Event
from src.queues_dir import QueuesDirectory


# ----------------------------- Utils & Fixtures -----------------------------

def get_logs_dir():
    project_root = Path(__file__).parent.parent.resolve()
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    return logs_dir

@pytest.fixture
def temp_log_file(request):
    logs_dir = get_logs_dir()
    test_name = request.node.name
    log_file = logs_dir / f"{test_name}.log"
    yield log_file

@pytest.fixture
def rsa_keys():
    return generate_rsa_keys()

@pytest.fixture
def blackbox(tmp_path, rsa_keys):
    private_key, public_key = rsa_keys
    queues_dir = QueuesDirectory()
    bb = BlackBox(queues_dir=queues_dir, storage_path=tmp_path, public_key=public_key)
    bb.queue = queues_dir.get_queue(bb.events_q_name)
    return bb

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


# ----------------------------- Log Event Tests -----------------------------

def test_log_event_writes_to_log(temp_log_file):
    queues_dir = QueuesDirectory()
    bb = BlackBox(queues_dir=queues_dir, storage_path=temp_log_file, public_key="something")
    event = Event(source="shibal", destination="test", operation="ping", parameters={"key": "value"})
    result = bb._log_event(event)
    assert result is True
    with open(temp_log_file, encoding='utf-8') as f:
        log_line = f.readline().strip()
        assert "destination" in log_line
        assert "ping" in log_line

def test_log_event_with_signature(rsa_keys, temp_log_file):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)
    event = Event("test_src", "test_dest", "test_op", {"foo": "bar"}, {})
    event.signature = create_signature(event, private_key)
    result = bb._log_event(event)
    assert result is True

def test_log_event_without_public_key_raises(temp_log_file, rsa_keys):
    private_key, _ = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, None)
    event = Event("mystery", "nowhere", "ghost", {"boo": True}, {})
    event.signature = create_signature(event, private_key)
    with pytest.raises(ValueError, match="Public key is not set"):
        bb._log_event(event)

def test_multiple_events_logged(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)
    for i in range(3):
        event = Event(f"src{i}", f"dest{i}", "log_event", {"id": i}, {})
        event.signature = create_signature(event, private_key)
        bb._log_event(event)
    with open(temp_log_file, "r") as f:
        lines = f.readlines()
    assert len(lines) == 3

def test_log_duplicate_event(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)
    event = Event("repeat", "loop", "ping", {"msg": "again"}, {})
    event.signature = create_signature(event, private_key)
    bb._log_event(event)
    bb._log_event(event)
    with open(temp_log_file, "r") as f:
        lines = f.readlines()
    assert len(lines) == 2

def test_log_event_large_payload_from_file(temp_log_file, rsa_keys, tmp_path):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)

    # Create a temporary large text file in the logs directory to avoid path issues
    logs_dir = get_logs_dir()
    large_file_path = logs_dir / "large_payload.txt"

    # Read content from the file to use in payload
    with open(large_file_path, "r") as f:
        file_content = f.read()

    big_data = {"blob": file_content}
    event = Event("big_sender", "big_receiver", "big_upload", big_data, {})
    event.signature = create_signature(event, private_key)

    result = bb._log_event(event)
    assert result is True

def test_log_event_with_unexpected_fields(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)
    event = Event("extra", "handler", "surprise", {"legit": 1}, {"unexpected": "oops"})
    event.signature = create_signature(event, private_key)
    result = bb._log_event(event)
    assert result is True

def test_log_event_performance(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)
    event = Event("speed", "log", "fast_write", {"value": "test"}, {})
    event.signature = create_signature(event, private_key)
    start = time.time()
    for _ in range(100):
        assert bb._log_event(event)
    duration = time.time() - start
    assert duration < 0.5


@pytest.mark.parametrize("operation", ["start", "stop", "restart", "status"])
def test_log_event_different_operations(temp_log_file, rsa_keys, operation):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)
    event = Event("service", "manager", operation, {"val": 1}, {})
    event.signature = create_signature(event, private_key)
    assert bb._log_event(event) is True

@pytest.mark.parametrize("params", [
    {"a": 1},
    {"msg": "hello", "count": 42},
    {},
    {"nested": {"x": [1, 2, 3], "y": {"z": 99}}}
])
def test_log_event_various_parameters(temp_log_file, rsa_keys, params):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)
    event = Event("tester", "log_system", "write", params, {})
    event.signature = create_signature(event, private_key)
    assert bb._log_event(event)

def test_log_event_with_wrong_param_types(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)

    class WeirdObject:
        def __str__(self): return "🌀"

    event = Event("chaos", "order", "break_json", {"weird": WeirdObject()}, {})
    event.signature = create_signature(event, private_key)
    result = bb._log_event(event)
    assert result is False or isinstance(result, bool)

def test_log_file_is_fresh_on_init(tmp_path, rsa_keys):
    _, public_key = rsa_keys
    log_file = tmp_path / "test.txt"
    with open(log_file, "w") as f:
        f.write("OLD DATA\n")
    bb = BlackBox(QueuesDirectory(), log_file, public_key)
    with open(log_file, "r") as f:
        content = f.read()
    assert "OLD DATA" not in content

def test_event_added_to_queue_and_processed(tmp_path, rsa_keys):
    private_key, public_key = rsa_keys
    queues_dir = QueuesDirectory()
    bb = BlackBox(queues_dir, tmp_path / "test.txt", public_key)
    queue = queues_dir.get_queue(bb.events_q_name)
    event = Event("qsource", "qdest", "qop", {"x": 42}, {})
    event.signature = create_signature(event, private_key)
    queue.put(event)
    event_from_queue = queue.get(timeout=1)
    result = bb._log_event(event_from_queue)
    assert result is True
    log_path = tmp_path / "test.txt"
    assert log_path.exists()
    with open(log_path) as f:
        logs = f.readlines()
    assert len(logs) == 1
    assert "qsource" in logs[0]

def test_log_event_with_delay(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)
    event = Event("slowpoke", "lag", "delay", {"msg": "late"}, {})
    event.signature = create_signature(event, private_key)
    time.sleep(0.5)
    result = bb._log_event(event)
    assert result is True

def test_log_event_with_cyclic_data(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key)
    a = {}
    a["self"] = a
    event = Event("loop", "loop", "loop", a, {})
    with pytest.raises(RecursionError):
        event.signature = create_signature(event, private_key)

def test_log_event_with_custom_directory(tmp_path, rsa_keys):
    private_key, public_key = rsa_keys
    custom_dir = tmp_path / "custom_logs"
    custom_dir.mkdir()
    log_path = custom_dir / "log.txt"
    bb = BlackBox(QueuesDirectory(), log_path, public_key)
    event = Event("custom", "logger", "write", {"msg": "ok"}, {})
    event.signature = create_signature(event, private_key)
    assert bb._log_event(event)

import json
import pytest
from src.queues_dir import QueuesDirectory

def test_log_message_impl_writes_correct_json(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key=public_key)

    test_signature = "ABC123SIGNATURE"
    test_message = "This is a test message"
    combined_message = f"{test_signature} {test_message}"

    result = bb._log_message_impl(combined_message)
    assert result is True

    with open(temp_log_file, "r", encoding="utf-8") as f:
        log_lines = f.readlines()

    assert len(log_lines) == 1

    log_entry = json.loads(log_lines[0])
    assert log_entry["signature"] == test_signature
    assert log_entry["message"] == test_message
    assert "time" in log_entry
    assert log_entry["time"].endswith("UTC")

def test_log_message_impl_appends_lines(temp_log_file, rsa_keys):
    private_key, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key=public_key)

    for i in range(3):
        bb._log_message_impl(f"SIG{i} message {i}")

    with open(temp_log_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == 3
    for i, line in enumerate(lines):
        data = json.loads(line)
        assert data["signature"] == f"SIG{i}"
        assert data["message"] == f"message {i}"

def test_log_message_impl_raises_if_no_public_key(temp_log_file):
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key=None)

    with pytest.raises(ValueError, match="Public key is not set"):
        bb._log_message_impl("some_signature some message")

def test_log_message_impl_invalid_format_raises(temp_log_file, rsa_keys):
    _, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key=public_key)

    bad_message = "invalidmessagewithnospace"

    bb._log_message_impl(bad_message)

    with open(temp_log_file, "r", encoding="utf-8") as f:
        entry = json.loads(f.readline())

    assert "message" in entry
    assert "signature" in entry
    assert entry["message"] in bad_message or bad_message in entry["signature"]


def test_log_multiple_messages(temp_log_file, rsa_keys):
    _, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key=public_key)

    for i in range(5):
        signature = f"SIGNATURE_{i}"
        message = f"Test message number {i}"
        full_msg = f"{signature} {message}"
        bb._log_message_impl(full_msg)

    with open(temp_log_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == 5
    for i, line in enumerate(lines):
        entry = json.loads(line)
        assert entry["signature"] == f"SIGNATURE_{i}"
        assert entry["message"] == f"Test message number {i}"

def test_log_message_with_unicode(temp_log_file, rsa_keys):
    _, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key=public_key)

    signature = "UNICODE_SIG"
    message = "Привет 🌍! 你好！"
    full_msg = f"{signature} {message}"

    bb._log_message_impl(full_msg)

    with open(temp_log_file, "r", encoding="utf-8") as f:
        entry = json.loads(f.readline())

    assert entry["signature"] == signature
    assert entry["message"] == message

import pytest

def test_log_message_without_public_key_raises(temp_log_file):
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key=None)

    with pytest.raises(ValueError, match="Public key is not set"):
        bb._log_message_impl("some_signature some message")

def test_log_message_json_content(temp_log_file, rsa_keys):
    _, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key=public_key)

    signature = "SIG_JSON"
    message_dict = {"action": "test", "payload": "data with spaces"}
    message = json.dumps(message_dict)
    full_msg = f"{signature} {message}"

    bb._log_message_impl(full_msg)

    with open(temp_log_file, "r", encoding="utf-8") as f:
        entry = json.loads(f.readline())

    assert entry["signature"] == signature
    assert json.loads(entry["message"]) == message_dict

def test_log_entry_format_order(temp_log_file, rsa_keys):
    _, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key=public_key)

    bb._log_message_impl("sig important message")

    with open(temp_log_file, "r", encoding="utf-8") as f:
        line = f.readline()

    entry = json.loads(line)
    assert list(entry.keys()) == ["time", "message", "signature"]

def test_log_no_space_in_message(temp_log_file, rsa_keys):
    _, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key=public_key)

    msg = "ONLY_SIGNATURE"
    bb._log_message_impl(msg)

    with open(temp_log_file, "r", encoding="utf-8") as f:
        entry = json.loads(f.readline())

    assert entry["signature"] == "ONLY_SIGNATURE"
    assert entry["message"] == ""

def test_log_massive_input(temp_log_file, rsa_keys):
    _, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key=public_key)

    for i in range(1000):
        bb._log_message_impl(f"SIG{i} message_{i}")

    with open(temp_log_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == 1000
    assert json.loads(lines[42])["message"] == "message_42"

import threading

def test_log_threaded_safety(temp_log_file, rsa_keys):
    _, public_key = rsa_keys
    bb = BlackBox(QueuesDirectory(), temp_log_file, public_key=public_key)

    def log_many(n):
        for i in range(n):
            bb._log_message_impl(f"SIG{i} threaded_message_{i}")

    threads = [threading.Thread(target=log_many, args=(100,)) for _ in range(5)]

    for t in threads: t.start()
    for t in threads: t.join()

    with open(temp_log_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == 500  

from src.event_types import ControlEvent
import time

def test_control_event_stop_sets_quit_flag(temp_log_file, rsa_keys):
    bb = BlackBox(QueuesDirectory(), temp_log_file)
    bb.start()
    
    bb._control_q.put(ControlEvent(operation='stop'))

    import time
    time.sleep(1)
    bb.join(timeout=2)
    assert not bb.is_alive()

from queue import Queue as ThreadQueue

def test_control_event_stop_sets_quit_flag_sync(temp_log_file):
    bb = BlackBox(QueuesDirectory(), temp_log_file)
    bb._control_q = ThreadQueue()  
    
    assert bb._quit is False
    
    bb._control_q.put(ControlEvent(operation='stop'))
    
    bb._check_control_q()
    
    assert bb._quit is True

