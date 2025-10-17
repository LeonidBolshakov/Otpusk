# (с) Л. А. Большаков, 2025
from pathlib import Path
import logging
from enum import Enum, auto
import sys
from typing import Any
from SRC.accumulatevidops import AccumulateVidops
from SRC.filterhandler import FilteringHandler

logger = logging.getLogger(__name__)

# fmt: off
LEVEL_STR_TO_INT = {
    "DEBUG"     : logging.DEBUG,
    "INFO"      : logging.INFO,
    "WARNING"   : logging.WARNING,
    "ERROR"     : logging.ERROR,
    "CRITICAL"  : logging.CRITICAL,
}
DEFAULT_FILE_LEVEL = logging.INFO
DEFAULT_CONSOLE_LEVEL = logging.CRITICAL
# fmt: on


class HandlerLogger(Enum):
    """Перечисление типов обработчиков логов."""

    file = auto()  # Запись логов в файл
    console = auto()  # Вывод логов в консоль
    not_processed_vidops = auto()  # Сбор необработанных vidop


class TuneLogger:
    """
    Класс настройки системы логирования приложения.

    Выполняет настройку обработчиков (файл, консоль,
    накопитель необработанных vidop), и форматирование вывода.
    """

    def __init__(self, parameters: dict[str, Any]):
        """
        Инициализация параметров логирования.

        Args:
            parameters: словарь с параметрами конфигурации. Ключи словаря:
                - level_console: уровень вывода в консоль (str)
                - level_file: уровень вывода в файл (str)
                - service_text: идентификатор сервисного сообщения
                - file_log_path: путь к файлу лога (str)
        """
        self.level_errors: list[str] = []
        # Запоминание значений из словаря параметров
        self.log_level_console = self.level_str_int(
            parameters["level_console"], DEFAULT_CONSOLE_LEVEL
        )
        self.log_level_file = self.level_str_int(
            parameters["level_file"], DEFAULT_FILE_LEVEL
        )
        self.service_text = parameters["service_text"]

        # Создание объекта для накопления "необработанных" записей
        self.accumulate_vidops = AccumulateVidops(self.service_text)

        # Словарь обработчиков логирования
        file_log_path = parameters["file_log_path"]

        self.handlers_logger: dict[HandlerLogger, logging.Handler] = {
            HandlerLogger.file: self.create_file_handler(file_log_path),
            HandlerLogger.console: logging.StreamHandler(sys.stdout),
            HandlerLogger.not_processed_vidops: self.accumulate_vidops,
        }

        # Формат вывода логов
        self.log_format = "%(asctime)s - %(levelname)s - %(module)s - %(message)s"

    def setup_logging(self) -> None:
        """Основная точка входа — настройка глобального логирования."""
        handlers = self.configure_handlers(
            self.log_format, self.log_level_console, self.log_level_file
        )
        # Добавление обработчиков к корневому логгеру
        self.configure_root_handlers(handlers)

    def create_file_handler(self, file_log_path: str) -> logging.FileHandler:
        """
        Создаёт файловый обработчик логов.

        Args:
            file_log_path: путь к файлу лога

        Returns:
            logging.FileHandler: настроенный обработчик
        """
        p = Path(file_log_path)
        mode = "w"
        return logging.FileHandler(
            filename=p, mode=mode, encoding="utf-8-sig", delay=True
        )

    def configure_handlers(
        self, log_format: str, log_level_console: int, log_level_file: int
    ) -> list[logging.Handler]:
        """
        Настраивает все обработчики логов и добавляет их к корневому логгеру.

        Args:
            log_format: строка формата логов
            log_level_console: уровень логирования для консоли
            log_level_file: уровень логирования для файла
        """
        handlers = list(self.handlers_logger.values())
        print(type(self.handlers_logger.values()))

        # Установка форматирования для обработчиков
        fmt = logging.Formatter(log_format)
        self.handlers_logger[HandlerLogger.file].setFormatter(fmt)
        self.handlers_logger[HandlerLogger.console].setFormatter(fmt)

        # Присвоение уровней сообщений каждому обработчику
        self.handlers_logger[HandlerLogger.file].setLevel(log_level_file)
        self.handlers_logger[HandlerLogger.console].setLevel(log_level_console)
        self.handlers_logger[HandlerLogger.not_processed_vidops].setLevel(logging.DEBUG)

        return handlers

    def configure_root_handlers(self, handlers: list[logging.Handler]) -> None:
        """
        Присоединяет обработчики к корневому логгеру приложения.

        Args:
            handlers: список обработчиков
        """
        self._remove_loging()  # Очистка прежних обработчиков

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.NOTSET)

        # Добавление фильтрующих обработчиков
        root_logger.addHandler(
            FilteringHandler(
                self.handlers_logger[HandlerLogger.file], service_text=self.service_text
            )
        )
        root_logger.addHandler(
            FilteringHandler(
                self.handlers_logger[HandlerLogger.console],
                service_text=self.service_text,
            )
        )
        # Добавление накопителя необработанных сообщений
        root_logger.addHandler(self.handlers_logger[HandlerLogger.not_processed_vidops])

    @staticmethod
    def _remove_loging() -> None:
        """Удаляет все обработчики из корневого логгера."""
        logger_root = logging.getLogger()
        for handler in logger_root.handlers[:]:
            logger_root.removeHandler(handler)

    def get_accumulated_vidops(self) -> set[str]:
        """
        Возвращает список накопленных сообщений из обработчика AccumulateVidops.

        Returns:
            list[str]: накопленные сообщения
        """
        return self.accumulate_vidops.accumulate

    def level_str_int(
        self, level_str: str, level_default: int = logging.WARNING
    ) -> int:
        """
        Преобразует строку уровня логирования в числовое значение.

        Args:
            level_str: строка, обозначающая уровень ("INFO", "ERROR" и т.д.)
            level_default: уровень по умолчанию, если строка некорректна

        Returns:
            int: числовой уровень логирования
        """
        level_int = LEVEL_STR_TO_INT.get(level_str)
        if level_int:
            return level_int
        # При ошибке фиксируется неверное значение
        self.level_errors.append(level_str)
        self.log_errors()
        return level_default

    def log_errors(self) -> None:
        """Логирует ошибки при указании некорректных уровней в конфигурации."""
        if self.level_errors:
            logging.error(
                f"\nВ cfg файле недопустимое значение/значения уровня логирования - "
                f"{', '.join(self.level_errors)}\nПрименяем уровень логирования по умолчанию"
            )
