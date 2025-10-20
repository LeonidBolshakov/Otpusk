"""OTPUSK: обработка записей UCHRABVR и генерация SQL-обновлений.
(с) Л. А. Большаков, 2025

Назначение
---------
Программа используется для отката выделения РКСН из отпускных и средних,
чтобы начислить на них НДФЛ по шкале надбавок.

Как это работает

1. Система Галактика определяет отпускные и средние, которые нужно скорректировать.
2. Она формирует и выгружает файл предварительной разноски с записями,
    подлежащими корректировке — UCHRABVR.txt.
3. Программа обрабатывает этот файл:
    * В основных видах оплат проставляются суммы,
        равные ранее начисленным по видам оплат РКСН.
    * Формируются SQL-запросы для корректировки таблицы UCHRABVR.

В терминологии программы основные начисления называются "основными",
а начисляемые на них надбавки (РКСН) "вторичными"

Скрипт читает файл `UCHRABVR.txt`(предварительная разноска),
группирует строки по сотруднику (`clsch`),
По «вторичным» видам оплат ищет «основные» по правилу `VARIABLE_VIDOPS`,
суммирует значения и формирует список `UPDATE`‑операторов корректировки
оплат предварительной разноске Галактики.

Основные понятия
----------------
* Uchrabvr: единица входных данных (строка из исходного файла).
* VARIABLE_VIDOPS: таблица соответствий основных ↔ вторичных кодов оплат.

Особенности и ограничения
-------------------------
* Денежные вычисления выполняются с `Decimal` и округлением до копеек.
* Конфигурация читается из `otpusk.cfg`; при отсутствии файла конфигурации или параметров
  используются значения по умолчанию.
* Логирование настраивается через `TuneLogger` (отдельный модуль).

Пример запуска
--------------
См блок `if __name__ == "__main__"` в конце файла. Формируемые SQL‑операторы
записываются в файл, заданный в настройках (по умолчанию `galaktika.lot`).
"""

from configparser import ConfigParser
from typing import NamedTuple, Any, Iterable, Iterator
import sys
from decimal import Decimal, ROUND_HALF_EVEN, getcontext

# Точность операций Decimal (значащих цифр). Для финансовых расчётов достаточно.
getcontext().prec = 28

from pathlib import Path
import csv
import logging

from SRC.tune_logger import TuneLogger

# ==== Модели данных ===========================================================
# fmt: off
class Uchrabvr(NamedTuple):
    """Строка UCHRABVR из входного файла.

    Поля соответствуют колонкам CSV (порядок фиксирован).
    Все значения поступают как строки и интерпретируются логикой ниже.
    """
    nrec        : str
    tabn        : str
    mes         : str
    mesn        : str
    vidop       : str
    summa       : str
    summaval    : str
    datan       : str
    datok       : str
    clsch       : str


class PrimarySecondaryCodes(NamedTuple):
    """Пара соответствия: основной(е) код(ы) ↔ вторичный(е) код(ы).

    Оба поля допускают либо одну строку, либо кортеж строк.
    """
    primary     : tuple["str", ...] | str
    secondary   : tuple["str", ...] | str

# fmt: on

# ==== Константы и тексты ======================================================
CONFIG_FILE_PATH = "otpusk.cfg"
DEFAULT_INPUT_FILE = "UCHRABVR.txt"
DEFAULT_OUTPUT_FILE = "update.lot"
DEFAULT_LOG_FILE = "otpusk.log"
DEFAULT_SQL_FILE = "update.lot"
SERVICE_TEXT = "*** | ***"  # маркер служебных сообщений в логе
ZERO = "0.00"

TEXT_ERROR = [
    # 0
    "Вид оплаты {vidop}, дата начала {datan}, дата окончания {datok} "
    "не имеет основного вида оплаты {main_vidop}",
    # 1
    "Вид оплаты {vidop}, дата начала {datan}, дата окончания {datok} "
    "имеет больше одного основного вида оплаты {main_vidop}",
    # 2
    "Вид оплаты {vidop}. Сумма или ранее вычисленная сумма у надбавки\n"
    "не преобразуется в число с плавающей запятой {summa_1} {summa_2}",
    # 3
    "Код вида вторичной оплаты в VARIABLE_VIDOPS не целое число - {vidop}",
    # 4 — текст не менять, соответствует SERVICE_TEXT в лог-обработчиках
    "Необработанный вид оплаты {service_text} {vidop}",
    # 5
    "Виды операций, не обработанные программой {vidops}.\n"
    "Возможно не заданы в VARIABLE_VIDOPS",
    # 6
    "Файл конфигураций {config_file} не найден.\n"
    "Будут использоваться значения по умолчанию.",
    # 7
    "При выполнении программы были зафиксированы ошибки.\n" "Подробности в log файле.",
]

WARNING_TEXT = (
    "\n1. Вызовите Систему Галактика"
    "\n2. Проверьте значение 2 настроек: Возвращать налог (Да), контролировать удержание (Нет)"
    "\n3. Выполните перерасчёт средних --> "
    "\n       Заработная плата | Настройка | Сервисные функции | Перерасчет средних"
    "\n4. Выполните предварительную разноску -->"
    "\n       Заработная плата | Операции | Расчет зарплаты  | Предварительная разноска"
    "\n5. Выполните bat файл select_razn.bat"
    "\n6. В этом окне нажмите клавишу Enter и дождитесь завершения работы программы"
    "\n7. Проанализируйте файл otpusk.log"
    "\n8. Выполните bat файл update_razn.bat"
    "\n9. Установите алгоритм 2 у следующих видов оплаты {vidops} -->"
    "\n        Заработная плата | Настройка | Заполнение каталогов | Виды оплат и скидок"
    "\n10. Вызовите расчёт зарплаты, НЕ ставьте галочку 'Предварительная разноска'"
    "\n11.После расчёта зарплаты, верните прежние значения алгоритмов."
)

# Соответствия основных и вторичных кодов оплат.
# fmt: off
VARIABLE_VIDOPS = (
    PrimarySecondaryCodes(("18", "48", "87", "204") , ("305", "306")),
    PrimarySecondaryCodes( "20"                     , ("315", "316")),
    PrimarySecondaryCodes( "54"                     , ("313", "314")),
    PrimarySecondaryCodes( "76"                     , ("309", "310")),
    PrimarySecondaryCodes( "77"                     , ("311", "312")),
    PrimarySecondaryCodes( "106"                    , ("303", "304")),
    PrimarySecondaryCodes(("107", "111")            , ("307", "308")),
    PrimarySecondaryCodes(("108", "109")            , ("318", "319")),
    PrimarySecondaryCodes(("104", "110", "112")     , ("303", "304")),
)
# fmt: on


class Otpusk:
    """Основной класс обработки.

    Жизненный цикл:
      1) `start()` — читает входной файл и по `clsch` группирует строки сотрудника,
         передавая группы в `processing_person()`.
      2) `processing_person()` — для сотрудника запускает весь конвейер:
         `processing_vidops()` → `create_SQL_request()` → контроль.
      3) `stop()` — Завершение: вывод неохваченных видов и остановка логгера.
    """

    def __init__(self) -> None:
        # Внутренние накопители по одному сотруднику
        self.person_uchrabvr: list[Uchrabvr] = []
        self.processed_vidops: set[str] = set()
        # Глобальный список SQL-операторов UPDATE
        self.SQL_queries: list[str] = []

        # Конфигурация, параметры и код возврата
        self.return_code = 0
        self.config: ConfigParser = ConfigParser()
        self.parameters: dict[str, str] = {}
        self.fill_in_parameters()

        # Логирование
        self.tune_logger: TuneLogger = TuneLogger(parameters=self.parameters)
        self.tune_logger.setup_logging()

        # Стартовое предупреждение с перечнем основных кодов
        self.service_warning()

    # ==== Внешний цикл обработки ============================================
    def start(self) -> None:
        """Главный вход: чтение, группировка по `clsch`, обработка групп."""
        # Сбрасываем суммы в 0.00 на уровне потока строк
        all_uchrabvr: Iterable[Uchrabvr] = (
            row._replace(summa=ZERO) for row in self.input_uchrabvr()
        )

        current_clsch = "-1"
        for uchrabvr in all_uchrabvr:
            # Смена сотрудника → обработать накопленную группу
            if uchrabvr.clsch != current_clsch:
                self.processing_person()
                current_clsch = uchrabvr.clsch
                self.person_uchrabvr = []
            self.person_uchrabvr.append(uchrabvr)
        # Хвост
        self.processing_person()

    # ==== Обработка одной группы (сотрудника) ================================
    def processing_person(self) -> None:
        """Полный конвейер для одного сотрудника."""
        if not self.person_uchrabvr:
            return

        self.processing_vidops()
        self.create_SQL_request()
        self.control_processing_completion()

    def processing_vidops(self) -> None:
        """Для каждой строки ищем вторичные коды и обновляем соответствующий основной."""
        self.processed_vidops.clear()
        for row in self.person_uchrabvr:
            for variable in VARIABLE_VIDOPS:
                if row.vidop in variable.secondary:
                    self.update_uchrabvr(row, variable.primary)
                    break

    def create_SQL_request(self) -> None:
        """Формируем SQL для строк, помеченных к записи (`write_down`)."""
        for row in self.person_uchrabvr:
            if row.summa != ZERO:
                self.SQL_queries.append(
                    f"UPDATE uchrabvr WHERE nrec={row.nrec} SET summa:={row.summa};"
                )

    def control_processing_completion(self) -> None:
        """Заносим в журнал неохваченные строки сотрудника (служебный формат)."""
        for uchrabvr in self.person_uchrabvr:
            if uchrabvr.vidop not in self.processed_vidops:
                self.error(
                    "-----",
                    TEXT_ERROR[4].format(
                        service_text=SERVICE_TEXT, vidop=uchrabvr.vidop
                    ),
                )
                self.error(
                    uchrabvr.tabn,
                    TEXT_ERROR[4].format(service_text="", vidop=uchrabvr.vidop),
                    logging.WARNING,
                )
                self.return_code = 1

    # ==== Внутренняя логика ==================================================
    def update_uchrabvr(
        self, uchrabvr: Uchrabvr, primary_vidops: str | tuple[str, ...]
    ) -> None:
        """Найти соответствующий основной код и прибавить `summaval` вторичной строки к `summa` основной.
        Основной код ищется по vidops, взятому из VARIABLE_VIDOPS, дате начала и дате окончания оплаты.

        Если подходящих основных строк 0 или >1 — Заносим в журнал ошибку с пояснением.
        """

        # Копим все индексы подходящих основных строк
        nums_in_person_uchrabvr = self.find_uchrabvr(
            primary_vidops, uchrabvr.datan, uchrabvr.datok
        )

        if len(nums_in_person_uchrabvr) == 0:
            # Нет ни одного основного — это ошибка
            self.error(uchrabvr.tabn, self.prepare_string(uchrabvr, 0, primary_vidops))
            self.return_code = 1
            return
        if len(nums_in_person_uchrabvr) > 1:
            # Несколько основных для одной вторичной строки — неоднозначность
            self.error(uchrabvr.tabn, self.prepare_string(uchrabvr, 1, primary_vidops))
            self.return_code = 1
            return

        # Ровно одна основная строка — обновляем её сумму
        self.update_primary_uchrabvr(nums_in_person_uchrabvr[0], uchrabvr)

    def find_uchrabvr(
        self,
        primary_vidops: Iterable[str] | str,
        datan: str,
        datok: str,
    ) -> list[int]:
        """Найти индексы строк с vidop и совпадающими датами"""
        # Нормализация primary_vidops
        primary_vidops = (
            (primary_vidops,) if isinstance(primary_vidops, str) else primary_vidops
        )

        # Основной блок
        nums_in_person_uchrabvr: list[int] = []
        for num_in_person_uchrabvr, row in enumerate(self.person_uchrabvr):
            for primary_vidop in primary_vidops:
                if (
                    row.vidop == primary_vidop
                    and row.datan == datan
                    and row.datok == datok
                ):
                    nums_in_person_uchrabvr.append(num_in_person_uchrabvr)
        return nums_in_person_uchrabvr

    def update_primary_uchrabvr(
        self, num_in_person_uchrabvr: int, secondary_uchrabvr: Uchrabvr
    ) -> None:
        """Прибавить `summaval` вторичной строки к `summa` найденной основной."""
        uchrabvr = self.person_uchrabvr[num_in_person_uchrabvr]
        self.processed_vidops.add(uchrabvr.vidop)
        self.processed_vidops.add(secondary_uchrabvr.vidop)
        try:
            summa = self.sum_str(uchrabvr.summa, secondary_uchrabvr.summaval)
        except ValueError:
            self.error(
                secondary_uchrabvr.tabn,
                TEXT_ERROR[2].format(
                    vidop=uchrabvr.vidop,
                    summa_1=uchrabvr.summa,
                    summa_2=secondary_uchrabvr.summaval,
                ),
            )
            raise
        updated_uchrabvr = uchrabvr._replace(summa=summa)
        self.person_uchrabvr[num_in_person_uchrabvr] = updated_uchrabvr

    def sum_str(self, s1: str, s2: str) -> str:
        """Суммировать две суммы в строковом представлении и округлить до копеек.

        Используется банковское округление `ROUND_HALF_EVEN`.
        """
        s_decimal = Decimal(s1) + Decimal(s2)
        return str(s_decimal.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN))

    def prepare_string(
        self, row: Uchrabvr, num_error: int, main_vidop: str | tuple[str, ...]
    ) -> str:
        """Собрать сообщение об ошибке по шаблону."""
        if isinstance(main_vidop, str):
            main_vidop = (main_vidop,)
        return TEXT_ERROR[num_error].format(
            vidop=row.vidop,
            datan=row.datan,
            datok=row.datok,
            main_vidop=" или ".join(main_vidop),
        )

    # ==== Обвязка I/O и сервис ==============================================
    def error(self, tabn: str, text_error: str, level_log: int = logging.ERROR) -> None:
        """Записать ошибку в лог общим форматом."""
        logging.log(level_log, f"Табельный номер {tabn}\n{text_error}")

    def output_result(self) -> list[str]:
        """Вернуть сформированные SQL-запросы для записи в файл."""
        return self.SQL_queries

    def input_uchrabvr(self) -> Iterator[Uchrabvr]:
        """Построчное чтение входного и преобразование к `Uchrabvr`."""
        file = Path(self.parameters["input_file_path"])
        try:
            with open(file, "r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                for row in reader:
                    yield Uchrabvr(*row)
        except (FileNotFoundError, PermissionError) as e:
            logging.critical(
                f"Либо неверно указан файл, выгруженный из Галактики, либо он не доступен\n{e}"
            )
            sys.exit(1)

    def fill_in_parameters(self) -> None:
        """Загрузить настройки из CFG и заполнить словарь `parameters`."""
        self.parameters["service_text"] = SERVICE_TEXT

        if not Path(CONFIG_FILE_PATH).exists():
            self.error("-----", TEXT_ERROR[6].format(config_file=CONFIG_FILE_PATH))
            self.return_code = 1

        self.config.read(CONFIG_FILE_PATH, encoding="utf-8")
        self.from_cfg_to_param("level_console", "LOG", logging.CRITICAL)
        self.from_cfg_to_param("level_file", "LOG", logging.INFO)
        self.from_cfg_to_param("file_log_path", "FILES", DEFAULT_LOG_FILE)
        self.from_cfg_to_param("input_file_path", "FILES", DEFAULT_INPUT_FILE)
        self.from_cfg_to_param("output_file_path", "FILES", DEFAULT_SQL_FILE)

    def from_cfg_to_param(
        self, name_parameter: str, section: str, default: Any
    ) -> None:
        self.parameters[name_parameter] = self.config.get(
            section, name_parameter, fallback=default
        )

    def service_warning(self) -> None:
        """Служебное предупреждение с перечнем необходимых действий.

        Выводится один раз в начале, затем ожидается подтверждение пользователя.
        """
        all_vidops_primary: set[int] = set()
        for row in VARIABLE_VIDOPS:
            vidops = (row.primary,) if isinstance(row.primary, str) else row.primary
            for vidop in vidops:
                all_vidops_primary.add(int(vidop))

        all_vidops_str_sorted = (str(vidop) for vidop in sorted(all_vidops_primary))
        logging.critical(WARNING_TEXT.format(vidops=", ".join(all_vidops_str_sorted)))
        input("Для продолжения работы нажмите клавишу Enter")

    def stop(self) -> None:
        """Завершение работы: вывести неохваченные виды и закрыть логирование."""
        accumulate_vidops = self.tune_logger.get_accumulated_vidops()
        if accumulate_vidops:
            self.error(
                "-----", TEXT_ERROR[5].format(vidops=", ".join(accumulate_vidops))
            )
        logging.critical("\nПрограмма закончила свою работу\n")


if __name__ == "__main__":
    # Точка входа CLI: создаём объект, запускаем обработку, пишем результат.
    otpusk = Otpusk()
    otpusk.start()
    otpusk.stop()

    out_path = Path(otpusk.parameters.get("output_file_path", DEFAULT_OUTPUT_FILE))

    try:
        with open(out_path, "w", encoding="cp866", newline="\r\n") as f_:
            f_.write("\n".join(otpusk.output_result()))
    except (FileNotFoundError, PermissionError) as ex:
        otpusk.error(
            "-----",
            f"Указан неверный путь на файл вывода SQL {out_path} или к нему нет доступа.\n{ex}",
        )
        otpusk.return_code = 1

    if otpusk.return_code:
        otpusk.error("-----", TEXT_ERROR[7], logging.CRITICAL)

    logging.shutdown()
    exit(otpusk.return_code)
