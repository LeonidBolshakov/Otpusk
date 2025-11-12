from typing import NamedTuple, Any
from pathlib import Path
import logging
from logging import getLogger
from configparser import ConfigParser

from SRC.common import error

logger = getLogger(__name__)

MSG_CFG_NOT_FOUND = (
    "Файл конфигураций {config_file} не найден.\n"
    "Будут использоваться значения по умолчанию."
)


class RequiredParameter(NamedTuple):
    section_name: str
    default_value: str


class Parameters:
    """
    Класс для:
      * загрузки конфигурации из CFG (без интерполяции значений);
      * заполнения словаря строковых параметров для других модулей;
    """

    def __init__(
        self,
        parameters: dict[str, Any],
        config_file_path: str,
        required_parameters: dict[str, RequiredParameter],
    ) -> None:
        self.config = ConfigParser(interpolation=None)
        self.parameters = parameters
        self.return_code = 0
        self._fill_in_parameters(config_file_path, required_parameters)

    def get_parameters(self) -> dict[str, Any]:
        return self.parameters

    def get_return_code(self) -> int:
        return self.return_code

    def _fill_in_parameters(
        self, config_file_path: str, required_parameters: dict[str, RequiredParameter]
    ) -> None:
        """
        Загружает параметры из CFG и заполняет `self.parameters` строковыми значениями.

        Если необходимых параметров нет в CFG в `self.parameters` переносятся
        значения по умолчанию и self.ret_code присваивается значение 1.
        """
        cfg_path = Path(config_file_path)

        if not cfg_path.exists():
            # Сообщаем об отсутситвии файла параметров и продолжаем с дефолтами
            error(
                "-----",
                MSG_CFG_NOT_FOUND.format(config_file=config_file_path),
                level_log=logging.WARNING,
            )
            # заполняем дефолты
            for name, req in required_parameters.items():
                self.parameters[name] = req.default_value
            self.return_code = 1

        # Читаем содержимое CFG в self.config
        self.config.read(config_file_path, encoding="utf-8")

        # Переносим значения (или дефолты) в parameters
        for name, req in required_parameters.items():
            self._from_cfg_to_param(name, req.section_name, req.default_value)

    def _from_cfg_to_param(
        self, name_parameter: str, section: str, default: str
    ) -> None:
        """Перенос параметров из config в словарь параметров"""
        # Замена отсутствующих секций/опций выполняется через fallback; всё храним как str.
        value = self.config.get(section, name_parameter, fallback=default)
        self.parameters[name_parameter] = str(value)
