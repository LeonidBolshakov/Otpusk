from typing import NamedTuple
from pathlib import Path
import csv
import logging
from enum import IntFlag, auto

from tune_logger import TuneLogger

# fmt: off
# noinspection SpellCheckingInspection
class Uchrabvr(NamedTuple):
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


# noinspection SpellCheckingInspection
class PrimarySecondaryCodes(NamedTuple):
    primary     : tuple["str", ...] | str
    secondary   : tuple["str", ...] | str


# noinspection SpellCheckingInspection
class StatusUchrabvr(IntFlag):
    processed = auto()
    write_down = auto()
# fmt: on


# noinspection SpellCheckingInspection
TEXT_ERROR = [
    "Вид оплаты {vidop}, дата начала {datan}, дата окончания {datok} "
    "не имеет основного вида оплаты {main_vidop}",  # 0
    "Вид оплаты {vidop}, дата начала {datan}, дата окончания {datok} "
    "имеют больше одного основного вида оплаты {main_vidop}",  # 1
    "Вид оплаты {vidop}. Сумма или ранее вычисленная сумма у надбавки\n"
    "не преобразуется в число с плавающей запятой {summa_1} {summa_2}",  # 2
    "Код вида вторичной оплаты в VARIABLE_VIDOPS не целое число - {vidop}",  # 3
    "*** | *** {vidop}",  # 4
]
# noinspection SpellCheckingInspection
# fmt: off
VARIABLE_VIDOPS: list[PrimarySecondaryCodes] = [
    PrimarySecondaryCodes(("18", "48", "87"), ("305", "306")),
    PrimarySecondaryCodes( "20"             , ("315", "316")),
    PrimarySecondaryCodes( "54"             , ("313", "314")),
    PrimarySecondaryCodes( "76"             , ("309", "310")),
    PrimarySecondaryCodes( "77"             , ("311", "312")),
    PrimarySecondaryCodes( "106"            , ("303", "304")),
    PrimarySecondaryCodes( "107"            , ("307", "308")),
    PrimarySecondaryCodes(("108", "109")    , ("318", "319")),
]
# fmt: on

# noinspection SpellCheckingInspection
INPUT_FILE_PATH = r"UCHRABVR.txt"


# noinspection SpellCheckingInspection
class Otpusk:
    def __init__(self) -> None:
        # fmt: off
        self.person_uchrabvr    : list[Uchrabvr] = []
        self.person_status      : dict[str, StatusUchrabvr] = {}
        self.SQL_queries        : list[str] = []
        # fmt: on

        self.init_logging()
        self.algoritms_warning()

    def start(self):
        all_uchrabvr: list[Uchrabvr] = []
        row: Uchrabvr
        all_uchrabvr = (row._replace(summa="0.00") for row in self.input_uchrabvr())

        clsch = "-1"
        for uchrabvr in all_uchrabvr:
            if uchrabvr.clsch != clsch:
                self.processing_person()
                clsch = uchrabvr.clsch
                self.person_uchrabvr = []
                self.person_status = {}
            self.person_uchrabvr.append(uchrabvr)
        self.processing_person()

    def processing_person(self) -> None:
        if not self.person_uchrabvr:
            return

        self.processing_vidops()
        self.create_SQL_request()
        self.control_complet_processing()

    def processing_vidops(self) -> None:
        for row in self.person_uchrabvr:
            for variable in VARIABLE_VIDOPS:
                if row.vidop in variable.secondary:
                    self.person_status[row.nrec] = StatusUchrabvr.processed
                    self.update_uchrabvr(row, variable.primary)

    def create_SQL_request(self) -> None:
        for row in self.person_uchrabvr:
            if self.person_status.get(row.nrec) == StatusUchrabvr.write_down:
                self.SQL_queries.append(
                    f"UPDATE secondary_uchrabvr WHERE nrec={row.nrec} SET summa:={row.summa};"
                )

    def control_complet_processing(self) -> None:
        print(self.person_status)
        for uchrabvr in self.person_uchrabvr:
            status = self.person_status.get(uchrabvr.nrec)
            if not status:
                self.error("*** | ***", TEXT_ERROR[4].format(vidop=uchrabvr.vidop))

    def update_uchrabvr(
        self, uchrabvr: Uchrabvr, primary_vidops: str | tuple[str, ...]
    ) -> None:
        if isinstance(primary_vidops, str):
            primary_vidops = (primary_vidops,)

        nums_in_person_uchrabvr: list[int] = []

        nums_in_person_uchrabvr = self.find_uchrabvr(
            nums_in_person_uchrabvr, primary_vidops, uchrabvr.datan, uchrabvr.datok
        )
        if len(nums_in_person_uchrabvr) == 0:
            self.error(uchrabvr.tabn, self.prepare_string(uchrabvr, 0, primary_vidops))
            return
        if len(nums_in_person_uchrabvr) > 1:
            self.error(uchrabvr.tabn, self.prepare_string(uchrabvr, 1, primary_vidops))
            return
        self.update_primary_uchrabvr(nums_in_person_uchrabvr[0], uchrabvr)

    def find_uchrabvr(
        self,
        nums_in_person_uchrabvr: list[int],
        primary_vidops: list[str],
        datan: str,
        datok: str,
    ) -> list[int]:

        for num_in_person_uchrabvr, row in enumerate(self.person_uchrabvr):
            for primary_vidop in primary_vidops:
                if (
                    row.vidop == primary_vidop
                    and row.datan == datan
                    and row.datok == datok
                ):
                    nums_in_person_uchrabvr.append(num_in_person_uchrabvr)
                    self.person_status[row.nrec] = StatusUchrabvr.processed
        return nums_in_person_uchrabvr

    def update_primary_uchrabvr(
        self, num_in_person_uchrabvr: int, secondary_uchrabvr: Uchrabvr
    ) -> None:
        uchrabvr = self.person_uchrabvr[num_in_person_uchrabvr]
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
        self.person_status[uchrabvr.nrec] = StatusUchrabvr.write_down
        self.person_uchrabvr[num_in_person_uchrabvr] = updated_uchrabvr

    def sum_str(self, s1: str, s2: str) -> str:
        s_float = float(s1) + float(s2)
        return f"{s_float:.2f}"

    def prepare_string(
        self, row: Uchrabvr, num_error: int, main_vidop: str | tuple[str, ...]
    ) -> str:
        if isinstance(main_vidop, str):
            main_vidop = (main_vidop,)

        return TEXT_ERROR[num_error].format(
            vidop=row.vidop,
            datan=row.datan,
            datok=row.datok,
            main_vidop=", ".join(main_vidop),
        )

    def error(self, tabn: str, text_error: str) -> None:
        logging.error(f"Табельный номер {tabn}\n{text_error}")

    def output_result(self) -> list[str]:
        return self.SQL_queries

    # noinspection SpellCheckingInspection
    def input_uchrabvr(self) -> list[Uchrabvr]:
        file = Path(INPUT_FILE_PATH)
        with open(file, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                yield Uchrabvr(*row)

    def init_logging(self) -> None:
        tune_logger = TuneLogger()
        tune_logger.setup_logging()

    def algoritms_warning(self):
        all_vidops_primary = set()
        for row in VARIABLE_VIDOPS:
            vidops = (row.primary,) if isinstance(row.primary, str) else row.primary
            for vidop in vidops:
                all_vidops_primary.add(int(vidop))

        all_vidops_str_sorted = (str(vidop) for vidop in sorted(all_vidops_primary))
        logging.critical(
            f"\n1. Вызовите Систему Галактика"
            f"\n2. Выполните --> Заработная плата | Настройка | Сервисные функции | Перерасчет средних"
            f"\n3. Выполните предварительную разноску"
            f"\n4. Выполните bat файл select_razn.bat"
            f"\n5. В окне этой программы нажмимте клавишу Enter и дождитесь завершения работы программы"
            f"\n6. Выполните bat файл update_razn.bat"
            f"\n7. Установите алгоритм 2 у следующих видов оплаты {', '.join(all_vidops_str_sorted)}"
            f"\n8. Вызовите расчёт зарплаты, НЕ ставьте галочку 'Предварительная разноска'"
            f"\n9. После расчёта зарплаты, верните прежние значения алгоритмов."
        )
        input("Для продолжения работы нажмите клавишу Enter")

    def stop(self):
        logging.shutdown()


if __name__ == "__main__":
    # noinspection SpellCheckingInspection
    otpusk = Otpusk()
    otpusk.start()
    with open("galaktika.sql", "w", encoding="cp866", newline="\r\n") as f_:
        f_.write("\n".join(otpusk.output_result()))
