import pytest
import logging
from io import StringIO
from typing import NamedTuple, Any
from configparser import ConfigParser

from SRC.common import Common, RequiredParameter


class Rows(NamedTuple):
    s1: str
    s2: str
    s3: str


@pytest.mark.parametrize(
    "level_log_base, level_log_message, is_log_message, tabn, message",
    [
        (logging.WARNING, logging.ERROR, True, 12345, "Сообщение"),
        (logging.ERROR, logging.ERROR, True, "*****", "Message"),
        (logging.ERROR, logging.WARNING, False, 12345, "Сообщение"),
    ],
)
def test_error(
    level_log_base, level_log_message, is_log_message, tabn, message, caplog
):
    logging.basicConfig(level=level_log_base)
    with caplog.at_level(level_log_base):
        Common.error(tabn, message, level_log_message)
    if is_log_message:
        assert str(message) in caplog.text
        assert str(tabn) in caplog.text
    else:
        assert not caplog.text


def test_input_table(monkeypatch):
    def mock_open(*args, **kwargs):
        return StringIO(rows_string)

    def mock_file_not_found(*args, **kwargs):
        raise FileNotFoundError()

    rows = [
        ("11", "12", "13"),
        ("21", "22", "23"),
        ("31", "32", "33"),
    ]
    rows_string = "\n".join(",".join(row) for row in rows)
    monkeypatch.setattr("builtins.open", mock_open)
    for i, row in enumerate(Common.input_table("Something", Rows)):
        assert row == rows[i]

    monkeypatch.setattr("builtins.open", mock_file_not_found)
    with pytest.raises(FileNotFoundError):
        list(Common.input_table("Something", Rows))


def test_fill_in_parameters_no_cfg(tmp_path, caplog):
    config_file_path = tmp_path / "no_such_file.ini"
    assert not config_file_path.exists()

    parameters: dict[str, Any] = {}
    required_parameters = {
        "param1": RequiredParameter(section_name="main", default_value="DEF1"),
        "param2": RequiredParameter(section_name="main", default_value="DEF2"),
    }

    obj = Common(parameters=parameters)

    with caplog.at_level(logging.WARNING):
        rc = obj.fill_in_parameters(
            config_file_path=config_file_path, required_parameters=required_parameters
        )

    assert "не найден" in caplog.text
    assert rc == 1
    assert obj.parameters == {"param1": "DEF1", "param2": "DEF2"}


def test_fill_inparameters_exists_cfg(caplog, tmp_path):
    """
    Тестирует   fill_in_parameters с заданным файлом конфигуратора и
                from_cfg_to_param
    :param caplog: стандартная фикстура PyTest - перехватывает log
    :param tmp_path: стандартная фикстура PyTest - получает временную пустую директорию
    :return: None
    """
    # Формируем файл конфигурации
    config = ConfigParser(interpolation=None)
    config["server"] = {"host": "localhost", "port": "8080"}
    config["client"] = {"timeout": 30}

    config_file_path = tmp_path / "exists_file.ini"
    with open(config_file_path, "w") as f:
        config.write(f)

    # Формируем параметры для Common
    parameters: dict[str, Any] = {}
    required_parameters = {
        "host": RequiredParameter("server", "127.0.0.1"),
        "port": RequiredParameter("server", "80"),
        "timeout": RequiredParameter("client", 60),
        "retries": RequiredParameter("client", 5),
    }

    # Объект класса Common
    obj = Common(
        parameters=parameters,
    )

    # Выпоняем тестируемую функцию
    with caplog.at_level(logging.WARNING):
        rc = obj.fill_in_parameters(
            config_file_path=config_file_path, required_parameters=required_parameters
        )

    # Проверякм результаты выполнения
    assert caplog.text == ""
    assert rc == 0
    assert obj.parameters == {
        "host": "localhost",
        "port": "8080",
        "timeout": "30",
        "retries": "5",  # Значение по умолчанию
    }


@pytest.mark.parametrize(
    "s1, s2, expected_result",
    [
        ("7", "1", "8.00"),
        ("5.321", "-3.617", "1.70"),
        ("3.t", "-3.617", "error"),
        ("7", 1, "error"),
    ],
)
def test_sum_str(s1, s2, expected_result):
    if expected_result == "error":
        with pytest.raises(ValueError):
            Common.sum_str(s1, s2)
    else:
        assert Common.sum_str(s1, s2) == expected_result
