"""UCHRABVR: перерасчёт предварительной разноски по UCHRABVR и генерация SQL-UPDATE.
(с) Л. А. Большаков, 2025

Назначение
---------
Программа корректирует суммы в «основных» видах оплат на основании «вторичных»
(РКСН) по правилу соответствий `PRIMARY_SECONDARY_PAYCODES` и формирует список SQL-операторов.

Вход:
    * CSV-файл `UCHRABVR.txt` (кодировка cp866): поля соответствуют `UchrabvrStructure`.
Конфигурация:
    * `uchrabvr.cfg` — уровни логирования, пути, формат логов; при отсутствии —
      используются дефолты из `REQUIRED_PARAMETERS`.
Логирование:
    * через `TuneLogger`; служебный маркер `SERVICE_TEXT` используется для
      исключения сообщений из основного лога и параллельного сбора «неохваченных» vidop.

Пайплайн:
    1) Чтение и группировка записей по `clsch`.
    2) Для каждой группы: поиск соответствий вторичных→основных, суммирование.
    3) Формирование SQL: `UPDATE uchrabvr WHERE nrec=... SET summa:=...;`
    4) Журнализация неохваченных видов оплат (служебным форматом).

Пример запуска
--------------
См блок `if __name__ == "__main__"` в конце файла. Формируемые SQL‑операторы
записываются в файл, заданный в настройках (по умолчанию `galaktika.lot`).
"""

from typing import NamedTuple, Iterable
from collections import defaultdict
import logging

from SRC.common import (
    Common,
    RequiredParameter,
    PRIMARY_SECONDARY_PAYCODES,
    PrimarySecondaryCodes,
)


# ==== Модели данных ===========================================================


class UchrabvrStructure(NamedTuple):
    """Строка UCHRABVR из входного файла.

    Поля соответствуют колонкам CSV (порядок фиксирован).
    Все значения поступают как строки и интерпретируются логикой ниже.
    """

    # fmt: off
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


# fmt: on


# ==== Константы и тексты ======================================================
# fmt: off
SERVICE_TEXT        = "*** | ***"  # маркер служебных сообщений в логе
ZERO                = "0.00"
CONFIG_FILE_PATH    = "uchrabvr.cfg"

REQUIRED_PARAMETERS: dict[str, RequiredParameter] = {
    "level_console"         : RequiredParameter("LOG", "CRITICAL"),
    "level_file"            : RequiredParameter("LOG", "INFO"),
    "file_log_path"         : RequiredParameter("FILES", "uchrabvr.log"),
    "input_file_uchrabvr"   : RequiredParameter("FILES", "UCHRABVR.txt"),
    "output_file_path"      : RequiredParameter("FILES", "uchrabvr_update.lot"),
    "log_format"            : RequiredParameter(
        "LOG", "%(asctime)s - %(levelname)s - %(module)s - %(message)s"
    ),
}
# fmt: on

TEXT_ERROR = [
    # 0
    "Вид оплаты {vidop}, дата начала {datan}, дата окончания {datok}"
    "\nне имеет основного вида оплаты {main_vidop}",
    # 1
    "Вид оплаты {vidop}, дата начала {datan}, дата окончания {datok}"
    "\nимеет больше одного основного вида оплаты {main_vidop}",
    # 2
    "Вид оплаты {vidop}. Сумма или ранее вычисленная сумма у надбавки"
    "\nне преобразуется в число с плавающей запятой {summa_1} {summa_2}",
    # 3
    "Ошибка в программе."
    "\nВ PRIMARY_SECONDARY_PAYCODES дублируются следующие коды secondary {codes}."
    "\nДальнейшая работа невозможна",
    # 4 — текст не менять, соответствует SERVICE_TEXT в лог-обработчиках
    "Необработанный вид оплаты {service_text} {vidop}",
    # 5
    "Виды операций, не обработанные программой {vidops}."
    "\nВозможно не заданы в PRIMARY_SECONDARY_PAYCODES",
    # 6
    "При выполнении программы были зафиксированы ошибки." "\nПодробности в log файле.",
    # 7
    "Программа завершена пользователем",
    # 8
    "Указан неверный путь на файл вывода операторов SQL {out_path} или к нему нет доступа.\n{ex}",
]

WARNING_TEXT = (
    "\n1. Вызовите Систему Галактика"
    "\n2. Проверьте значение 2 настроек: Возвращать налог (Да), контролировать удержание (Нет)"
    "\n3. Выполните перерасчёт средних --> "
    "\n       Заработная плата | Настройка | Сервисные функции | Перерасчет средних"
    "\n4. Выполните предварительную разноску -->"
    "\n       Заработная плата | Операции | Расчет зарплаты  | Предварительная разноска"
    "\n5. Выполните bat файл uchrabvr_select.bat"
    "\n6. В этом окне нажмите клавишу Enter и дождитесь завершения работы программы"
    "\n7. Проанализируйте файл uchrabvr.log"
    "\n8. Выполните bat файл uchrabvr_update.bat"
    "\n9. Установите алгоритм 2 у следующих видов оплаты {vidops} -->"
    "\n        Заработная плата | Настройка | Заполнение каталогов | Виды оплат и скидок"
    "\n10. Вызовите расчёт зарплаты, НЕ ставьте галочку 'Предварительная разноска'"
    "\n11.После расчёта зарплаты, верните прежние значения алгоритмов."
)


class Uchrabvr:
    """Основной класс обработки.

    Жизненный цикл:
      1) `start()` — читает входной файл и по `clsch` группирует строки сотрудника,
         передавая группы в `processing_person()`.
      2) `processing_person()` — для сотрудника запускает весь конвейер:
         `processing_vidops()` → `create_SQL_request()` → контроль.
      3) `stop()` — Завершение: вывод неохваченных видов и остановка логгера.
    """

    def __init__(self) -> None:
        """
        Конструктор основного класса.

        Выполняет только подготовку внутреннего состояния, не запуская бизнес-логику.
        Инициализация разбита на независимые этапы:
          1. self._init_state()   — создание внутренних структур данных;
          2. self._init_config()  — загрузка параметров из конфигурации;
          3. self.common.init_logging() — настройка подсистемы логирования.

        Атрибут `return_code` используется для фиксации кода завершения программы
        (0 — успешное выполнение, 1 — были ошибки).
        """
        self.return_code: int = 0
        self._index_by_key = dict()
        self._init_config()
        self._init_validate()
        self._init_state()
        self.common.init_logging()

    def _init_state(self) -> None:
        """
        Создаёт и инициализирует внутренние структуры данных, которые
        накапливаются в ходе обработки одного сотрудника и всей программы.

        Атрибуты:
            person_uchrabvr  — список объектов UchrabvrStructure (входные строки для одного сотрудника);
            processed_vidops — множество кодов оплат, которые были обработаны;
            SQL_update_queries — итоговый список SQL-операторов UPDATE.
        """
        self.person_uchrabvr = []
        self.processed_vidops = set()
        self.SQL_update_queries = []

    def _init_config(self) -> None:
        """
        Загружает параметры из конфигурационного файла и
        связывает общий класс Common с текущим объектом.

        Действия:
            * создаётся ConfigParser и словарь parameters;
            * Common(config, parameters) читает uchrabvr.cfg или задаёт значения по умолчанию;
            * метод fill_in_parameters() возвращает код (0 — успех, 1 — предупреждение);
            * добавляется служебный текст, используемый при журнализации.

        Итог:
            self.parameters содержит пути к файлам, уровни логирования и прочие настройки.
        """

        self.parameters = {}
        self.common = Common(self.parameters)
        self.return_code = self.common.fill_in_parameters(
            CONFIG_FILE_PATH, REQUIRED_PARAMETERS
        )
        # SERVICE_TEXT добавляется в параметры — его заберёт TuneLogger для фильтрации/аккумуляции.
        self.parameters["service_text"] = SERVICE_TEXT

    # ==== Внешний цикл обработки ============================================
    def start(self) -> None:
        """Главный вход: чтение, группировка по `clsch`, обработка групп."""

        # Стартовое предупреждение с перечнем основных действий
        self.service_warning()

        # Создаём поток строк. Сбрасываем суммы в 0.00 на уровне потока
        file_uchrabvr = self.parameters["input_file_uchrabvr"]
        all_uchrabvr: Iterable[UchrabvrStructure] = (
            row._replace(summa=ZERO)
            for row in self.common.input_table(file_uchrabvr, UchrabvrStructure)
        )

        # Основной блок
        current_clsch = "-1"
        for uchrabvr in all_uchrabvr:
            # Смена сотрудника → обработать накопленную группу
            if uchrabvr.clsch != current_clsch:
                self.processing_person()
                current_clsch = uchrabvr.clsch
                self.person_uchrabvr.clear()
            self.person_uchrabvr.append(uchrabvr)
        # Хвост
        self.processing_person()

    # ==== Обработка одной группы (одного сотрудника) ================================
    def processing_person(self) -> None:
        """Полный конвейер для одного сотрудника."""
        if not self.person_uchrabvr:
            return

        self.create_index_by_key()
        self.processing_vidops()
        self.create_SQL_request()
        self.control_processing_completion()

    def create_index_by_key(self) -> None:
        self._index_by_key = defaultdict(list)
        for i_row, row in enumerate(self.person_uchrabvr):
            self._index_by_key[row.vidop, row.datan, row.datok].append(i_row)

    def processing_vidops(self) -> None:
        """Для каждой строки ищем вторичные коды и обновляем соответствующий основной."""
        self.processed_vidops.clear()
        for row in self.person_uchrabvr:
            for variable in PRIMARY_SECONDARY_PAYCODES:
                if row.vidop in variable.secondary:
                    self.update_uchrabvr(row, variable.primary)
                    break

    def create_SQL_request(self) -> None:
        """Формируем SQL для строк, помеченных к записи (`write_down`)."""
        for row in self.person_uchrabvr:
            if row.summa != ZERO:
                self.SQL_update_queries.append(
                    f"UPDATE uchrabvr WHERE nrec={row.nrec} SET summa:={row.summa};"
                )

    def control_processing_completion(self) -> None:
        """Заносим в журнал необработанные строки сотрудника (служебный формат)."""
        for uchrabvr in self.person_uchrabvr:
            if uchrabvr.vidop not in self.processed_vidops:
                self.common.error(
                    "-----",
                    TEXT_ERROR[4].format(
                        service_text=SERVICE_TEXT, vidop=uchrabvr.vidop
                    ),
                )
                self.common.error(
                    uchrabvr.tabn,
                    TEXT_ERROR[4].format(service_text="", vidop=uchrabvr.vidop),
                    logging.WARNING,
                )
                self.return_code = 1

    # ==== Внутренняя логика ==================================================
    def update_uchrabvr(
        self, uchrabvr: UchrabvrStructure, primary_vidops: str | tuple[str, ...]
    ) -> None:
        """Найти соответствующий основной код и прибавить `summaval` вторичной строки к `summa` основной.
        Основной код ищется по vidops, взятому из PRIMARY_SECONDARY_PAYCODES, дате начала и дате окончания оплаты.

        Если подходящих основных строк 0 или >1 — Заносим в журнал ошибку с пояснением.
        """

        # Копим все индексы подходящих основных строк
        nums_in_person_uchrabvr = self.find_uchrabvr(
            primary_vidops, uchrabvr.datan, uchrabvr.datok
        )

        if len(nums_in_person_uchrabvr) == 0:
            # Нет ни одного основного — ошибка
            self.common.error(
                uchrabvr.tabn, self.prepare_string(uchrabvr, 0, primary_vidops)
            )
            self.return_code = 1
            return
        if len(nums_in_person_uchrabvr) > 1:
            # Несколько основных для одной вторичной строки — неоднозначность
            self.common.error(
                uchrabvr.tabn, self.prepare_string(uchrabvr, 1, primary_vidops)
            )
            self.return_code = 1
            return

        # Ровно одна основная — обновляем её сумму
        self.update_primary_uchrabvr(nums_in_person_uchrabvr[0], uchrabvr)

    def find_uchrabvr(
        self,
        primary_vidops: Iterable[str] | str,
        datan: str,
        datok: str,
    ) -> list[int]:
        """Найти индексы строк с vidop и совпадающими датами"""
        # Нормализация primary_vidops
        primary_vidops = self.common.normalize_tuple_str(primary_vidops)

        # Основной блок
        nums_in_person_uchrabvr: list[int] = []
        for pv in primary_vidops:
            nums_in_person_uchrabvr.extend(
                self._index_by_key.get((pv, datan, datok), [])
            )

        return nums_in_person_uchrabvr

    def update_primary_uchrabvr(
        self, num_in_person_uchrabvr: int, secondary_uchrabvr: UchrabvrStructure
    ) -> None:
        """Прибавить `summaval` вторичной строки к `summa` найденной основной (Decimal через Common.sum_str)."""
        uchrabvr = self.person_uchrabvr[num_in_person_uchrabvr]
        self.add_vidops_to_processed_vidops(uchrabvr.vidop, secondary_uchrabvr.vidop)

        # Прибавляем `summaval` вторичной строки к `summa` найденной основной.
        try:
            summa = self.common.sum_str(uchrabvr.summa, secondary_uchrabvr.summaval)
        except ValueError:
            self.common.error(
                secondary_uchrabvr.tabn,
                TEXT_ERROR[2].format(
                    vidop=uchrabvr.vidop,
                    summa_1=uchrabvr.summa,
                    summa_2=secondary_uchrabvr.summaval,
                ),
            )
            raise

        # noinspection PyProtectedMember
        updated_uchrabvr = uchrabvr._replace(
            summa=summa
        )  # pylint: disable=protected-access
        self.person_uchrabvr[num_in_person_uchrabvr] = updated_uchrabvr

    def add_vidops_to_processed_vidops(self, vidop1: str, vidop2: str) -> None:
        """Добавляем vidop в список обработанных vidop"""
        self.processed_vidops.add(vidop1)
        self.processed_vidops.add(vidop2)

    def prepare_string(
        self, row: UchrabvrStructure, num_error: int, main_vidop: str | tuple[str, ...]
    ) -> str:
        """Собрать сообщение об ошибке по шаблону."""
        main_vidop = self.common.normalize_tuple_str(main_vidop)
        return TEXT_ERROR[num_error].format(
            vidop=row.vidop,
            datan=row.datan,
            datok=row.datok,
            main_vidop=" или ".join(main_vidop),
        )

    def _init_validate(self):
        """Валидация констант и входных данных"""
        codes = self.validate_unique_secondary_codes(PRIMARY_SECONDARY_PAYCODES)
        if codes:
            self.common.error("-----", TEXT_ERROR[3].format(codes=codes))
            raise ValueError

    def validate_unique_secondary_codes(
        self,
        variable_vidops: Iterable[PrimarySecondaryCodes],
    ) -> list[str]:
        """
        Проверка уникальности secondary кодов в PRIMARY_SECONDARY_PAYCODES
        :param variable_vidops: PRIMARY_SECONDARY_PAYCODES
        :return: Список дублирующихся кодов
        """
        all_codes = set()
        duplicate_codes: list[str] = []
        for row in variable_vidops:
            primary = self.common.normalize_tuple_str(row.primary)
            secondary = self.common.normalize_tuple_str(row.secondary)
            for code in primary + secondary:
                if code in all_codes:
                    duplicate_codes.append(code)
                else:
                    all_codes.add(code)

        return duplicate_codes

    # ==== Сервис ==============================================
    def output_result(self) -> list[str]:
        """Вернуть сформированные SQL-запросы."""
        return self.SQL_update_queries

    def service_warning(self) -> None:
        """
        Печатает пошаговые инструкции; формирует перечень основных vidop;
        ставит паузу `input()` перед стартом.
        """
        all_vidops_primary: set[int] = set()
        for row in PRIMARY_SECONDARY_PAYCODES:
            vidops = (row.primary,) if isinstance(row.primary, str) else row.primary
            for vidop in vidops:
                all_vidops_primary.add(int(vidop))

        all_vidops_str_sorted = (str(vidop) for vidop in sorted(all_vidops_primary))
        logging.critical(WARNING_TEXT.format(vidops=", ".join(all_vidops_str_sorted)))
        input("Для продолжения работы нажмите клавишу Enter")

    def stop(self) -> None:
        """Завершение работы: вывести неохваченные виды и закрыть логирование."""
        accumulate_vidops = self.common.tune_logger.get_accumulated_vidops()
        if accumulate_vidops:
            self.common.error(
                "-----", TEXT_ERROR[5].format(vidops=", ".join(accumulate_vidops))
            )
        logging.critical("\nПрограмма закончила свою работу\n")


if __name__ == "__main__":
    # Делегируем в CLI-обёртку.
    from SRC.uchrabvr_cli import main

    main()
