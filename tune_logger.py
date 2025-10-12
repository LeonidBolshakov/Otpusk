from pathlib import Path
import logging
from enum import Enum
import sys

from accumulate_not_procesed_vidops import AccumulateNotProcessedVidops
from filterhandler import FilteringHandler

logger = logging.getLogger(__name__)

# noinspection SpellCheckingInspection
LOG_FILE_PATH = "otpusk.log"


# Обозначения обработчиков логеров внутри класса
class HandlerLogger(Enum):
    file = "file"
    console = "console"
    # noinspection SpellCheckingInspection
    not_processed_vidops = "AccumulateNotProcessedVidops"


class TuneLogger:
    def __init__(self):
        """Инициализация с использованием переменных окружения"""
        self.console_log_level = logging.CRITICAL
        self.file_log_level = logging.INFO
        self.handlers_logger = {  # Словарь обработчиков логгеров
            HandlerLogger.file: self.create_file_handler(),
            HandlerLogger.console: logging.StreamHandler(sys.stdout),
            HandlerLogger.not_processed_vidops: AccumulateNotProcessedVidops(),
        }

        # Формат для всех обработчиков логгеров
        self.log_format = (
            "%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s"
        )

    def setup_logging(self):
        """Настройка глобального логирования"""

        # Настройка уровней логирования
        self.configure_handlers(
            self.log_format, self.console_log_level, self.file_log_level
        )

    @staticmethod
    def create_file_handler() -> logging.FileHandler:
        """Создание файлового обработчика"""

        p = Path(LOG_FILE_PATH)
        mode = "w"

        return logging.FileHandler(
            filename=p,
            mode=mode,
            encoding="utf-8-sig",
            delay=True,
        )

    def configure_handlers(
        self, log_format: str, log_level_console: int, file_log_level: int
    ) -> None:
        """Конфигурация всех обработчиков"""

        handlers = list(self.handlers_logger.values())

        # Настройка форматирования
        for handler in handlers:
            handler.setFormatter(logging.Formatter(log_format))

        # Настройка уровней логирования
        self.handlers_logger[HandlerLogger.file].setLevel(file_log_level)
        self.handlers_logger[HandlerLogger.console].setLevel(log_level_console)
        self.handlers_logger[HandlerLogger.not_processed_vidops].setLevel(
            logging.NOTSET
        )

        self.configure_root_handlers(handlers)

    def configure_root_handlers(self, handlers: list[logging.Handler]) -> None:
        """Добавление обработчиков к корневому логгеру"""
        self._remove_loging()  # Удаление всех прежних обработчиков

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)

        root_logger.addHandler(
            FilteringHandler(self.handlers_logger[HandlerLogger.file])
        )
        root_logger.addHandler(
            FilteringHandler(self.handlers_logger[HandlerLogger.console])
        )
        root_logger.addHandler(self.handlers_logger[HandlerLogger.not_processed_vidops])

    @staticmethod
    def _remove_loging() -> None:
        """Удаление настроек логирования"""
        logger_root = logging.getLogger()
        for handler in logger_root.handlers[:]:
            logger_root.removeHandler(handler)
