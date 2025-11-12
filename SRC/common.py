"""
Общие утилиты для чтения конфигурации, построчного чтения таблиц и денежных расчётов.

Особенности:
    * ConfigParser создаётся с `interpolation=None` — строки вида `%(...)s`
        читаются как есть.
    * `fill_in_parameters()` заполняет словарь параметров из CFG или дефолтами.
    * `input_table()` — ленивое чтение csv-файла с маппингом строк на тип `Table(*row)`.
    * `sum_str()` — надёжные денежные суммы с Decimal и округлением HALF_EVEN.
"""

from typing import NamedTuple, TypeVar, Type, Iterator
from decimal import Decimal, ROUND_HALF_EVEN, getcontext, InvalidOperation
from SRC.tune_logger import TuneLogger
import csv
import logging
from logging import getLogger

logger = getLogger(__name__)

T = TypeVar("T")

# Точность вычислений decimal
getcontext().prec = 28


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

# fmt: on


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


def normalize_tuple_str(tuple_str: tuple | str) -> tuple[str, ...]:
    """
    Данные типа ('s1', ...) или 's1' приводит к виду ('s1', ...).
    :param tuple_str: Данные типа ('s1', ...) или 's1'
    :return: ('s1', ...).
    """
    return (tuple_str,) if isinstance(tuple_str, str) else tuple(tuple_str)


def init_logging(parameters) -> TuneLogger:
    """
    Настраивает логирование через TuneLogger.
    Требует, чтобы parameters уже содержал нужные, для настройки логирования, ключи.
    """
    tune_logger = TuneLogger(parameters)
    tune_logger.setup_logging()

    return tune_logger


def input_table(file_table: str, Table: Type[T]) -> Iterator[T]:
    """
    Построчно читает CSV (кодировка cp866) и преобразует каждую строку в объект `Table`.

    Ожидается, что вызов `Table(*row)` валиден для каждой строки ввода.
    Исключения `FileNotFoundError`/`PermissionError` пробрасываются
    вызывающему коду.
    """
    try:
        with open(file_table, "r", newline="", encoding="cp866") as f:
            reader = csv.reader(f)
            for row in reader:
                yield Table(*row)
    except (FileNotFoundError, PermissionError) as e:
        logger.critical(
            f"Либо неверно указан файл, выгруженный из Галактики, либо он недоступен\n{e}"
        )
        raise


def error(tabn: str, text_error: str, level_log: int = logging.ERROR) -> None:
    """Записать ошибку/сообщение в лог общим форматом."""
    logger.log(level_log, f"Табельный номер {tabn} - {text_error}")
