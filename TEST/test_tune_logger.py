import pytest
import logging

from SRC.tune_logger import TuneLogger
from SRC.uchrabvr import Uchrabvr, UchrabvrStructure, REQUIRED_PARAMETERS
from SRC.common import (
    PRIMARY_SECONDARY_PAYCODES,
    PrimarySecondaryCodes,
    RequiredParameter,
)


def test_level_str_int_error(caplog, monkeypatch):
    monkeypatch.setitem(
        REQUIRED_PARAMETERS,
        "level_console",
        RequiredParameter("Network", "Bad"),
    )

    caplog.clear()
    with caplog.at_level(logging.WARNING):  # ловим root.warning
        obj = Uchrabvr()  # здесь сработает предупреждение при создании TuneLogger

    assert "Неизвестный уровень логирования" in caplog.text


def test_level_str_int(caplog, monkeypatch):
    parameters = {
        "level_console": "CRITICAL",
        "level_file": "INFO",
        "log_format": "%(message)s",
        "file_log_path": "uchrabvr.log",
    }
    tune_logger = TuneLogger(parameters)
    assert tune_logger.level_str_int("DEBUG", 101) == logging.DEBUG


class Bad:
    def __init__(self, value: str):
        pass

    def __str__(self):
        raise ValueError


@pytest.mark.parametrize(
    "param, result",
    [("21", 21), ("-17", None), ("21.3", None), (Bad("4"), "exception")],
)
def test_to_int_if_digit(param, result):
    if result == "exception":
        result = None

    assert TuneLogger._to_int_if_digit(param) == result
