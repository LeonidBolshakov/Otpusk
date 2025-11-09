"""
Общие утилиты для чтения конфигурации, построчного чтения таблиц и денежных расчётов.

Особенности:
    * ConfigParser создаётся с `interpolation=None` — строки вида `%(...)s`
        читаются как есть.
    * `fill_in_parameters()` заполняет словарь параметров из CFG или дефолтами.
    * `input_table()` — ленивое чтение csv-файла с маппингом строк на тип `Table(*row)`.
    * `sum_str()` — надёжные денежные суммы с Decimal и округлением HALF_EVEN.
"""

from typing import NamedTuple, Iterator, Any, Type, TypeVar
from pathlib import Path
import logging
import csv
from configparser import ConfigParser
from decimal import Decimal, ROUND_HALF_EVEN, getcontext, InvalidOperation

# Точность вычислений decimal
getcontext().prec = 28

from SRC.tune_logger import TuneLogger

T = TypeVar("T")

MSG_CFG_NOT_FOUND = (
    "Файл конфигураций {config_file} не найден.\n"
    "Будут использоваться значения по умолчанию."
)


# fmt: off
class PrimarySecondaryCodes(NamedTuple):
    """Пара соответствия: основной(е) код(ы) ↔ вторичный(е) код(ы).

    Используется для связывания видов оплат:
    - primary  — основные коды оплат (исходные начисления);
    - secondary — вторичные коды (районные коэффициенты и северные надбавки).
    """

    primary         : tuple[str, ...] | str
    secondary       : tuple[str, ...] | str


PRIMARY_SECONDARY_PAYCODES = (
    PrimarySecondaryCodes(("18", "48", "87", "204")     , ("305", "306")),
    PrimarySecondaryCodes("20"                          , ("315", "316")),
    PrimarySecondaryCodes("54"                          , ("313", "314")),
    PrimarySecondaryCodes("76"                          , ("309", "310")),
    PrimarySecondaryCodes("77"                          , ("311", "312")),
    PrimarySecondaryCodes(("106", "104", "110", "112")  , ("303", "304")),
    PrimarySecondaryCodes(("107", "111")                , ("307", "308")),
    PrimarySecondaryCodes(("108", "109")                , ("318", "319")),
)

class RequiredParameter(NamedTuple):
    section_name    : str
    default_value   : str
# fmt: on


class Common:
    """
    Вспомогательный класс для:
      * загрузки конфигурации из CFG (без интерполяции значений);
      * заполнения словаря строковых параметров для других модулей;
      * настройки логирования (через TuneLogger);
      * утилитарных операций (ввод таблиц, суммирование денежных значений).
    """

    def __init__(
        self,
        parameters: dict[str, Any],
    ) -> None:
        self.config = ConfigParser(interpolation=None)
        self.parameters = parameters
        self.tune_logger: TuneLogger | None = None

    @staticmethod
    def error(tabn: str, text_error: str, level_log: int = logging.ERROR) -> None:
        """Записать ошибку/сообщение в лог общим форматом."""
        logging.log(level_log, f"Табельный номер {tabn} - {text_error}")

    @staticmethod
    def input_table(file_table: str, Table: Type[T]) -> Iterator[T]:
        """
        Построчно читает CSV (кодировка cp866) и преобразует каждую строку в объект `Table`.

        Ожидается, что вызов `Table(*row)` валиден для каждой строки ввода.
        Исключения `FileNotFoundError`/`PermissionError` логируются и пробрасываются
        вызывающему коду.
        """
        try:
            with open(file_table, "r", newline="", encoding="cp866") as f:
                reader = csv.reader(f)
                for row in reader:
                    yield Table(*row)
        except (FileNotFoundError, PermissionError) as e:
            logging.critical(
                f"Либо неверно указан файл, выгруженный из Галактики, либо он недоступен\n{e}"
            )
            raise

    def fill_in_parameters(
        self, config_file_path: str, required_parameters: dict[str, RequiredParameter]
    ) -> int:
        """
        Загрузить настройки из CFG и заполнить `self.parameters` строковыми значениями.

        Возвращает:
            0 — если файл прочитан успешно;
            1 — если файла нет (использованы значения по умолчанию).
        """
        cfg_path = Path(config_file_path)

        if not cfg_path.exists():
            # Сообщаем и продолжаем с дефолтами
            self.error(
                "-----",
                MSG_CFG_NOT_FOUND.format(config_file=config_file_path),
                level_log=logging.WARNING,
            )
            # заполняем дефолты
            for name, req in required_parameters.items():
                self.parameters[name] = req.default_value
            return 1

        # Читаем в self.config
        self.config.read(config_file_path, encoding="utf-8")

        # Переносим значения (или дефолты) в parameters
        for name, req in required_parameters.items():
            self.from_cfg_to_param(name, req.section_name, req.default_value)

        return 0

    def init_logging(self) -> None:
        """
        Настраивает логирование через TuneLogger.
        Требует, чтобы self.parameters уже содержал нужные ключи.
        """
        self.tune_logger = TuneLogger(self.parameters)
        self.tune_logger.setup_logging()

    def from_cfg_to_param(
        self, name_parameter: str, section: str, default: str
    ) -> None:
        # Замена отсутствующих секций/опций выполняется через fallback; всё храним как str.
        value = self.config.get(section, name_parameter, fallback=default)
        self.parameters[name_parameter] = str(value)

    @staticmethod
    def sum_str(s1: str, s2: str) -> str:
        """
        Суммирует две суммы в строковом представлении и округляет до копеек
        по банковскому правилу ROUND_HALF_EVEN.
        """
        if not (isinstance(s1, str) and isinstance(s2, str)):
            raise ValueError
        try:
            s_decimal = Decimal(s1) + Decimal(s2)
        except InvalidOperation:
            raise ValueError

        return str(s_decimal.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN))

    @staticmethod
    def normalize_tuple_str(tuple_str: str) -> tuple[str, ...]:
        return (tuple_str,) if isinstance(tuple_str, str) else tuple(tuple_str)
