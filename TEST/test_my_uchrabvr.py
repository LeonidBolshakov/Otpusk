import pytest
import logging

from SRC.uchrabvr import Uchrabvr, UchrabvrStructure
from SRC.common import (
    PRIMARY_SECONDARY_PAYCODES,
    PrimarySecondaryCodes,
)
import SRC.common as common

# fmt: off
uchrabvr_structure = UchrabvrStructure(
    nrec        = "10",
    tabn        = "001",
    mes         = "01",
    mesn        = "Январь",
    vidop       = "18",
    summa       = "0.00",
    summaval    = "0.00",
    datan       = "2025-01-01",
    datok       = "2025-01-31",
    clsch       = "A",
)
# fmt: on


@pytest.fixture
def uchrabvr_obj(monkeypatch: pytest.MonkeyPatch) -> Uchrabvr:
    def input_from_buffer(*args, **kwargs):
        for row in obj.buffer:
            yield row

    obj = Uchrabvr()

    monkeypatch.setattr("builtins.input", lambda _: "")
    monkeypatch.setattr(common, "input_table", input_from_buffer)
    obj.buffer = []
    return obj


def test_validate_returns_list_type(uchrabvr_obj: Uchrabvr):
    dupes = uchrabvr_obj.validate_unique_secondary_codes(PRIMARY_SECONDARY_PAYCODES)
    assert isinstance(dupes, list)
    assert all(isinstance(x, str) for x in dupes)


def test_validate_detecates_on_broken_mapping(uchrabvr_obj):
    broken = (
        PrimarySecondaryCodes(("18",), ("305", "306")),
        PrimarySecondaryCodes(("20",), ("315", "316")),
        # намеренный дубль secondary "305"
        PrimarySecondaryCodes(("54",), ("305",)),
    )

    dupes = uchrabvr_obj.validate_unique_secondary_codes(broken)
    assert isinstance(dupes, list)
    assert "305" in dupes
    assert len([d for d in dupes if d == "305"])


def test_more_than_one_primary_vidop(monkeypatch, caplog, uchrabvr_obj: Uchrabvr):
    uchrabvr_obj.buffer = [
        # две основные (18 и 48 в одной группе primary для 305)
        uchrabvr_structure._replace(nrec="10", vidop="18"),
        uchrabvr_structure._replace(nrec="12", vidop="48"),
        # одна вторичная, которая должна маппиться на ту же группу
        uchrabvr_structure._replace(nrec="13", vidop="305"),
    ]

    caplog.clear()
    with caplog.at_level(logging.INFO):
        uchrabvr_obj.start()

    assert uchrabvr_obj.return_code == 1
    assert "имеет больше одного основного вида оплаты" in caplog.text


def test_absent_primary_vidop(monkeypatch, caplog, uchrabvr_obj: Uchrabvr):
    uchrabvr_obj.buffer = [
        # В основных нет ВО = 20
        uchrabvr_structure._replace(nrec="10", vidop="18"),
        # вторичные ВО 315 и 316 связаны с оновном ВО - 20
        uchrabvr_structure._replace(nrec="12", vidop="315"),
        uchrabvr_structure._replace(nrec="13", vidop="316"),
    ]

    caplog.clear()
    with caplog.at_level(logging.INFO):
        uchrabvr_obj.start()

    assert uchrabvr_obj.return_code == 1
    assert "не имеет основного вида оплаты" in caplog.text


def test_bad_sum(monkeypatch, caplog, uchrabvr_obj: Uchrabvr):
    uchrabvr_obj.buffer = [
        uchrabvr_structure._replace(nrec="10", vidop="20"),
        # Сумма не число сплавающей запятой
        uchrabvr_structure._replace(nrec="12", vidop="315", summaval="3t14"),
        uchrabvr_structure._replace(nrec="13", vidop="316"),
    ]

    caplog.clear()
    with caplog.at_level(logging.ERROR), pytest.raises(ValueError):
        uchrabvr_obj.start()

    assert "не преобразуется в число с плавающей запятой" in caplog.text


def test_unprocessed_payment_types(monkeypatch, caplog, uchrabvr_obj: Uchrabvr):
    uchrabvr_obj.buffer = [
        uchrabvr_structure._replace(nrec="10", vidop="20"),
        # Сумма не число сплавающей запятой
        uchrabvr_structure._replace(nrec="12", vidop="313", summaval="314"),
        uchrabvr_structure._replace(nrec="13", vidop="314"),
    ]

    caplog.clear()
    with caplog.at_level(logging.ERROR):
        uchrabvr_obj.start()

    assert "Необработанный вид оплаты" in caplog.text


def test_service_warning(monkeypatch, caplog, uchrabvr_obj: Uchrabvr):
    primary = PRIMARY_SECONDARY_PAYCODES[0].primary
    primary_code = primary if isinstance(primary, str) else primary[0]

    caplog.clear()
    with caplog.at_level(logging.ERROR):
        uchrabvr_obj.service_warning()

    caplog_text = caplog.text.strip()

    assert "Выполните bat файл uchrabvr_select.bat" in caplog_text
    assert "НЕ ставьте галочку 'Предварительная разноска'" in caplog_text
    assert primary_code in caplog_text
