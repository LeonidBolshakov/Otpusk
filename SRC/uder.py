"""
Проверка налоговых удержаний по месяцам (Uder).

Назначение:
    Считывает `UDER.txt`, нормализует коды/месяцы, группирует удержания по сотруднику
    и проверяет «налоговые» удержания (VIDOPS_OF_TAX): по каждой группе месячных записей
    суммирует значения и ожидает нулевой остаток. Ненулевые суммы фиксирует в логе.

Конфигурация:
    * `uder.cfg` (или значения по умолчанию `REQUIRED_PARAMETERS`).

Логирование:
    * настраивается через `Parameters.init_logging()`/`TuneLogger`.
"""

from configparser import ConfigParser
from typing import Iterable
from dataclasses import dataclass, asdict
import logging

from SRC.common import (
    PRIMARY_SECONDARY_PAYCODES,
    PrimarySecondaryCodes,
)
import SRC.common as common

from SRC.parameters import Parameters, RequiredParameter

# fmt: off
VIDOPS_OF_TAX           = ("13", "182")
CONFIG_FILE_PATH        = "uder.cfg"

REQUIRED_PARAMETERS     : dict[str, RequiredParameter] = {
    "level_console"     : RequiredParameter("LOG", "CRITICAL"),
    "level_file"        : RequiredParameter("LOG", "INFO"),
    "file_log_path"     : RequiredParameter("FILES", "uder.log"),
    "input_file_uder"   : RequiredParameter("FILES", "UDER.txt"),
    "last_mount"        : RequiredParameter("LIMITS", "6"),
    "log_format"        : RequiredParameter("LOG", "%(message)s"),
}


@dataclass(frozen=True, slots=True)
class UderStructure:
    """Строка входных данных UDER.txt (одна строка удержания).

    Поля соответствуют колонкам входного файла:
    """
    nrec                : str # идентификатор записи
    tabn                : str # табельный номер сотрудника
    mes                 : str # месяц начисления (как строка, может быть '7', '11', '')
    vidud               : str # код вида удержания
    sumud               : str # сумма удержания (строкой; суммируется как десятичная)
    clsch               : str # NREC сотрудника, по нему группируем «персону»
    datav               : str # дата выплаты
    vidoplud            : str # код вида оплаты (если присутствует)


# fmt: on


@dataclass(frozen=True, slots=True)
class UderGrouped(UderStructure):
    """Модификация записи удержания с добавленным ключом группировки по месяцу."""

    group_vidud: str


class Uder:
    """Загрузка параметров, настройка логирования, нормализация данных и проверка удержаний по группам."""

    def __init__(self) -> None:
        self.config = ConfigParser()
        self.parameters_dict: dict[str, str] = {}
        self.person_uders: list[UderStructure] = []

        # 1. Загрузка и валидация параметров
        parameters = Parameters(
            parameters=self.parameters_dict,
            config_file_path=CONFIG_FILE_PATH,
            required_parameters=REQUIRED_PARAMETERS,
        )
        self.return_code = parameters.get_return_code()

        # 2. Настройка логирования
        self._init_logging()

        # 3. Подготовка нормализованных данных
        self._normalize_data()

    def _init_logging(self) -> None:
        """Инициализирует логирование (консоль/файл) согласно параметрам."""
        common.init_logging(self.parameters_dict)

    def _normalize_data(self) -> None:
        """Кэширует таблицу соответствий PRIMARY_SECONDARY_PAYCODES в виде кортежей строк."""
        self.normalize_last_mount = self.normalize_mount(
            self.parameters_dict.get("last_mount", "")
        )
        self.normalized_variable_vidops = tuple(
            self.normalize_codes(row) for row in PRIMARY_SECONDARY_PAYCODES
        )

    def start(self) -> None:
        """Главный цикл: читает UDER.txt, накапливает удержания по сотруднику и обрабатывает группы."""
        file_uder = self.parameters_dict["input_file_uder"]
        all_uders: Iterable[UderStructure] = (
            row for row in common.input_table(file_uder, UderStructure)
        )

        logging.error(
            "Заполнитель 1; Табельный номер; Заполнитель 2; Месяц; Заполнитель 3; Разница сумм налогов"
        )
        current_clsch = None
        for uder in all_uders:
            # Смена сотрудника → сначала обработать накопленную группу (если она не пуста).
            if current_clsch is None:
                current_clsch = uder.clsch

            if uder.clsch != current_clsch:
                self.processing_person()
                self.person_uders.clear()
                current_clsch = uder.clsch

            self.person_uders.append(uder)

        # # Обработать «хвост» — накопленные записи последнего сотрудника.
        self.processing_person()

    def processing_person(self):
        """Фильтрует и сортирует записи по группам месяцов и валидирует их суммы."""
        filtered_uders = self.filter_sort_by_group()
        self.validate_person_groups(filtered_uders)

    def create_group_key(self, uder: UderStructure) -> str | None:
        """
        Возвращает ключ для группировки удержаний (месяц).
        Если удержание — НДФЛ (VIDOPS_OF_TAX) и месяц,
        за который произведено удержание, не больше предельного месяца,
        то возвращается месяц в формате ММ, иначе None.
        """
        normalize_mount = self.normalize_mount(uder.mes)
        if (
            uder.vidud not in VIDOPS_OF_TAX
            or self.normalize_last_mount < normalize_mount
        ):
            return None

        return normalize_mount

    def normalize_codes(self, codes: PrimarySecondaryCodes) -> PrimarySecondaryCodes:
        """Нормализует набор кодов: гарантирует, что primary/secondary представлены как кортежи строк."""
        return PrimarySecondaryCodes(
            primary=common.normalize_tuple_str(codes.primary),
            secondary=common.normalize_tuple_str(codes.secondary),
        )

    def normalize_mount(self, mount: str) -> str:
        """
        Приводит строку с номером месяца к формату "MM": '','7','12','11...' → '00','07','12','11'.
        """
        if len(mount) == 0:
            return "00"
        if len(mount) == 1:
            return ("0" + mount)[0:2]
        return mount[0:2]

    def filter_sort_by_group(self) -> list[UderGrouped]:
        """Отбирает удержания, для которых есть ключ группы, и сортирует их по месяцу (group_vidud)."""
        filtered_uders: list[UderGrouped] = []
        for uder in self.person_uders:
            group_key = self.create_group_key(uder)
            if group_key is not None:
                filtered_uders.append(
                    UderGrouped(**asdict(uder), group_vidud=group_key)
                )

        filtered_uders.sort(key=lambda row: row.group_vidud)
        return filtered_uders

    def check_summa(self, summa: str, uder: UderGrouped | None) -> None:
        """Логирует ненулевую сумму для группы (месяца) конкретного сотрудника."""
        if uder is None:
            return
        if summa != "0.00":
            mount = uder.group_vidud
            logging.info(
                f"Табельный номер; {uder.tabn}; Месяц; {mount}; Разница сумм налогов =; {summa}"
            )

    def validate_person_groups(self, filtered_uders: list[UderGrouped]) -> None:
        # noinspection GrazieInspection
        """
        Проверяет удержания по всем группам у одной персоны:
        суммирует значения в каждой группе и сверяет, что итог равен нулю.
        Смена ключа → проверка суммы предыдущей группы; в конце дополнительная проверка "хвоста".
        """
        current_group = None
        summa = "0.00"
        for i_uder, uder in enumerate(filtered_uders):
            if current_group is None:
                current_group = uder.group_vidud

            if current_group != uder.group_vidud:
                self.check_summa(summa, filtered_uders[i_uder - 1])
                summa = "0.00"
                current_group = uder.group_vidud
            summa = common.sum_str(summa, uder.sumud)

        if len(filtered_uders) != 0:
            self.check_summa(summa, filtered_uders[-1])


if __name__ == "__main__":
    uder_ = Uder()
    uder_.start()
