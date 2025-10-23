"""
(с) Л. А. Большаков, 2025
TuneLogger: сборка и настройка подсистемы логирования.

Состав:
    * Преобразование уровней (строка/число → int).
    * Создание обработчиков: файл, консоль, и `AccumulateVidops` для сбора служебных vidop.
    * Оборачивание файла/консоли в `FilteringHandler` для исключения сообщений с `service_text`.

Особенности:
    * Формат лога берётся как есть (из параметров), предварительно интерполяция в INI отключена в `Common`.
    * Каталог под файл лога создаётся при инициализации (_build_handlers).
"""

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
DEFAULT_SERVICE_TEXT = "*** | ***"
# fmt: on


class HandlerLogger(Enum):
    """Перечисление типов обработчиков логов."""

    file = auto()  # Запись логов в файл
    console = auto()  # Вывод логов в консоль
    not_processed_vidops = auto()  # Сбор «необработанных» vidop (служебный обработчик)


class TuneLogger:
    def __init__(self, parameters: dict[str, Any]):
        self.level_errors: list[str] = []

        # 1) Разбор параметров с дефолтами и нормализацией
        self.log_level_console = self.level_str_int(
            self._normalize_level(parameters.get("level_console")),
            DEFAULT_CONSOLE_LEVEL,
        )
        self.log_level_file = self.level_str_int(
            self._normalize_level(parameters.get("level_file")), DEFAULT_FILE_LEVEL
        )
        self.service_text = parameters.get("service_text") or DEFAULT_SERVICE_TEXT

        self.log_format = (parameters.get("log_format") or "").strip()
        if not self.log_format:
            raise ValueError("log_format is required and must be non-empty")

        self.file_log_path = (parameters.get("file_log_path") or "").strip()
        if not self.file_log_path:
            raise ValueError("file_log_path is required and must be non-empty")

        # 2) Подготовка вспомогательных объектов
        self.accumulate_vidops = AccumulateVidops(self.service_text)

        # 3) Создание handlers (без присоединения к root)
        self.handlers_logger: dict[HandlerLogger, logging.Handler] = (
            self._build_handlers()
        )

    # === Helpers ===
    @staticmethod
    def _normalize_level(level: Any) -> str | None:
        if level is None:
            return None
        if isinstance(level, int):
            return str(level)
        return str(level).strip().upper()

    def _build_handlers(self) -> dict[HandlerLogger, logging.Handler]:
        # гарантируем каталог
        Path(self.file_log_path).parent.mkdir(parents=True, exist_ok=True)

        file_handler = self.create_file_handler(self.file_log_path)
        console_handler = logging.StreamHandler(sys.stdout)

        # формат сразу
        fmt = logging.Formatter(self.log_format)
        file_handler.setFormatter(fmt)
        console_handler.setFormatter(fmt)

        file_handler.setLevel(self.log_level_file)
        console_handler.setLevel(self.log_level_console)
        self.accumulate_vidops.setLevel(logging.DEBUG)

        return {
            HandlerLogger.file: file_handler,
            HandlerLogger.console: console_handler,
            HandlerLogger.not_processed_vidops: self.accumulate_vidops,
        }

    def setup_logging(self) -> None:
        handlers = list(self.handlers_logger.values())
        self.configure_root_handlers(handlers)

    def create_file_handler(self, file_log_path: str) -> logging.FileHandler:
        # Режим "w": файл лога перезаписывается при каждом запуске; encoding без BOM
        return logging.FileHandler(
            filename=Path(file_log_path), mode="w", encoding="utf-8", delay=True
        )

    def configure_root_handlers(self, handlers: list[logging.Handler]) -> None:
        self._remove_logging()
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.NOTSET)

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
        root_logger.addHandler(self.handlers_logger[HandlerLogger.not_processed_vidops])

    @staticmethod
    def _remove_logging() -> None:
        logger_root = logging.getLogger()
        for handler in logger_root.handlers[:]:
            logger_root.removeHandler(handler)

    def get_accumulated_vidops(self) -> set[str]:
        """Возвращает набор (set) накопленных сообщений из AccumulateVidops."""
        return self.accumulate_vidops.accumulate

    def level_str_int(
        self, level_str: str | None, level_default: int = logging.WARNING
    ) -> int:
        # поддержим числа и строки
        if level_str is None:
            return level_default
        try:
            # если пришёл "20" или 20
            if str(level_str).isdigit():
                return int(level_str)
        except Exception:
            pass

        level_int = LEVEL_STR_TO_INT.get(str(level_str).upper())
        if level_int is not None:
            return level_int

        self.level_errors.append(str(level_str))
        self.log_errors()
        return level_default

    def log_errors(self) -> None:
        """Логирует неизвестные уровни; если обработчики ещё не подключены — печатает в stderr."""
        if not self.level_errors:
            return
        msg = f"Неизвестные уровни логирования: {', '.join(self.level_errors)}"
        # Если логгер уже настроен, используем его — иначе fallback на stderr
        logger_ = logging.getLogger()
        if logger_.hasHandlers():
            logger_.warning(msg)
        else:
            print(msg, file=sys.stderr)
