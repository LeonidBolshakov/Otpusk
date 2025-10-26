"""
Проверка удержаний по месяцам (CheckResult).

Назначение:
    Считывает `UDER.txt`, нормализует коды/месяцы, группирует удержания по сотруднику
    и проверяет «налоговые» удержания (VIDOPS_OF_TAX): по каждой группе месячных записей
    суммирует значения и ожидает нулевой остаток. Ненулевые суммы фиксирует в логе.

Конфигурация:
    * `uder.cfg` (или значения по умолчанию `REQUIRED_PARAMETERS`).

Логирование:
    * настраивается через `Common.init_logging()`/`TuneLogger`.
"""

from configparser import ConfigParser
from typing import Iterable
from dataclasses import dataclass, asdict
import logging


from SRC.common import Common, VARIABLE_VIDOPS, PrimarySecondaryCodes, RequiredParameter
from SRC.otpusk import SERVICE_TEXT


# fmt: off
@dataclass(frozen=True, slots=True)
class Uder:
    nrec:       str
    tabn:       str
    mes:        str
    vidud:      str
    sumud:      str
    clsch:      str
    datav:      str
    vidoplud:   str
# fmt: on


@dataclass(frozen=True, slots=True)
class UderGrouped(Uder):
    group_vidud: str


VIDOPS_OF_TAX = ("13", "182")
CONFIG_FILE_PATH = "uder.cfg"

REQUIRED_PARAMETERS: dict[str, RequiredParameter] = {
    "level_console": RequiredParameter("LOG", "CRITICAL"),
    "level_file": RequiredParameter("LOG", "INFO"),
    "file_log_path": RequiredParameter("FILES", "uder.log"),
    "input_file_uder": RequiredParameter("FILES", "UDER.txt"),
    "last_mount": RequiredParameter("LIMITS", "6"),
    "log_format": RequiredParameter("LOG", "%(message)s"),
}


class CheckResult:
    """Загрузка параметров, настройка логирования, нормализация данных и проверка удержаний по группам."""

    def __init__(self) -> None:
        self.config = ConfigParser()
        self.parameters: dict[str, str] = {}
        self.person_uders: list[Uder] = []

        # 1. Инициализация общих компонентов
        self.common = Common(CONFIG_FILE_PATH, self.parameters, REQUIRED_PARAMETERS)

        # 2. Загрузка и валидация параметров
        self.return_code = self._init_parameters()

        # 3. Настройка логирования
        self._init_logging()

        # 4. Подготовка нормализованных данных
        self._normalize_data()

    def _init_parameters(self) -> int:
        return self.common.fill_in_parameters()

    def _init_logging(self) -> None:
        # SERVICE_TEXT добавляется в параметры — его заберёт TuneLogger. Для этого модуля — заглушка
        self.parameters["service_text"] = SERVICE_TEXT
        self.common.init_logging()

    def _normalize_data(self) -> None:
        """Кэширует нормализованный предел месяца и таблицу соответствий VARIABLE_VIDOPS в виде кортежей строк."""
        self.normalize_last_mount = self.normalize_mount(
            self.parameters.get("last_mount", "")
        )
        self.normalized_variable_vidops = tuple(
            self.normalize_codes(row) for row in VARIABLE_VIDOPS
        )

    def start(self) -> None:
        file_uder = self.parameters["input_file_uder"]
        all_uders: Iterable[Uder] = (
            row for row in self.common.input_table(file_uder, Uder)
        )

        logging.error(
            "Заполнитель 1; Табельный номер; Заполнитель 2; Месяц; Заполнитель 3; Разница сумм налогов"
        )
        current_clsch = None
        for uder in all_uders:
            # смена сотрудника → обработать накопленную группу (если она не пуста)
            if current_clsch is None:
                current_clsch = uder.clsch

            if uder.clsch != current_clsch:
                self.processing_person()
                self.person_uders.clear()
                current_clsch = uder.clsch

            self.person_uders.append(uder)

        # обработать «хвост»
        self.processing_person()

    def processing_person(self):
        filtered_uders = self.filter_sort_by_group()
        self.validate_person_groups(filtered_uders)

    def create_group_key(self, uder: Uder) -> str | None:
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

        return PrimarySecondaryCodes(
            primary=self.to_tuple(codes.primary),
            secondary=self.to_tuple(codes.secondary),
        )

    @staticmethod
    def to_tuple(value: str | tuple[str, ...]) -> tuple[str, ...]:
        if isinstance(value, str):
            return (value,)
        return value

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
            summa = self.common.sum_str(summa, uder.sumud)

        if len(filtered_uders) != 0:
            self.check_summa(summa, filtered_uders[-1])


if __name__ == "__main__":
    check_result = CheckResult()
    check_result.start()
