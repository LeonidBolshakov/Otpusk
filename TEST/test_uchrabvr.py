import sys
import runpy
import builtins
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # -> C:\2_otpusk

import logging
import pytest

from SRC.uchrabvr import (
    Uchrabvr,
    UchrabvrStructure,
)
from SRC.common import PRIMARY_SECONDARY_PAYCODES, PrimarySecondaryCodes


@pytest.fixture
def uchrabvr_obj(monkeypatch, tmp_path):
    # Избегает input() паузы в service_warning()
    monkeypatch.setattr(Uchrabvr, "service_warning", lambda self: None)
    obj = Uchrabvr()
    # Безопасный путь к временному журналу
    obj.parameters["file_log_path"] = str(tmp_path / "uchrabvr.log")
    obj.common.init_logging()
    return obj


def test_validate_returns_list_type(uchrabvr_obj):
    """Функция должна возвращать список (list[str]), даже если дубликатов нет."""
    dupes = uchrabvr_obj.validate_unique_secondary_codes(PRIMARY_SECONDARY_PAYCODES)
    assert isinstance(
        dupes, list
    ), "Ожидали list[str], а вернулось не-список (возможно None)"
    # Проверка типов элементов:
    assert all(isinstance(x, str) for x in dupes), "Элементы результата должны быть str"


def test_validate_detects_duplicates_on_broken_mapping(uchrabvr_obj):
    """
    Намеренно создаём таблицу с дублем secondary-кода и ожидаем,
    что validate_unique_secondary_codes вернёт его.
    """
    broken = (
        PrimarySecondaryCodes(("18",), ("305", "306")),
        PrimarySecondaryCodes(("20",), ("315", "316")),
        # намеренный дубль secondary "305"
        PrimarySecondaryCodes(("54",), ("305",)),
    )
    dupes = uchrabvr_obj.validate_unique_secondary_codes(broken)
    assert isinstance(dupes, list), "Функция должна возвращать список, а не None"
    assert "305" in dupes, "Ожидали увидеть '305' в списке дубликатов"
    # Уникальность списка дубликатов:
    assert (
        len([d for d in dupes if d == "305"]) == 1
    ), "Дубликаты должны возвращаться без повторов"


def _rows_for_update():
    datan, datok = "2025-01-01", "2025-01-31"
    primary = UchrabvrStructure(
        nrec="10",
        tabn="001",
        mes="01",
        mesn="Январь",
        vidop="18",
        summa="0.00",
        summaval="0.00",
        datan=datan,
        datok=datok,
        clsch="A",
    )
    secondary = UchrabvrStructure(
        nrec="11",
        tabn="001",
        mes="01",
        mesn="Январь",
        vidop="305",
        summa="0.00",
        summaval="100.00",
        datan=datan,
        datok=datok,
        clsch="A",
    )
    other = UchrabvrStructure(
        nrec="20",
        tabn="002",
        mes="01",
        mesn="Январь",
        vidop="999",
        summa="0.00",
        summaval="0.00",
        datan=datan,
        datok=datok,
        clsch="B",
    )
    return [primary, secondary, other]


def test_processing_updates_primary_and_generates_sql(
    monkeypatch, caplog, uchrabvr_obj
):
    def _fake_input_table(file_table, Table):
        return (r for r in rows)

    rows = _rows_for_update()
    monkeypatch.setattr(uchrabvr_obj.common, "input_table", _fake_input_table)

    with caplog.at_level(logging.DEBUG):
        uchrabvr_obj.start()
        uchrabvr_obj.stop()

    sql = uchrabvr_obj.output_result()
    assert len(sql) == 1
    assert sql[0] == "UPDATE uchrabvr WHERE nrec=10 SET summa:=100.00;"


def test_validate_unique_secondary_codes_no_duplicates(uchrabvr_obj):
    dupes = uchrabvr_obj.validate_unique_secondary_codes(PRIMARY_SECONDARY_PAYCODES)
    assert dupes == []


def test_error_when_no_primary_found(monkeypatch, uchrabvr_obj):
    datan, datok = "2025-01-01", "2025-01-31"
    only_secondary = [
        UchrabvrStructure(
            nrec="11",
            tabn="001",
            mes="01",
            mesn="Январь",
            vidop="305",
            summa="0.00",
            summaval="50.00",
            datan=datan,
            datok=datok,
            clsch="A",
        )
    ]
    monkeypatch.setattr(
        uchrabvr_obj.common,
        "input_table",
        lambda file, Table: (r for r in only_secondary),
    )
    uchrabvr_obj.start()
    assert uchrabvr_obj.return_code == 1


def test_ambiguous_primary_for_single_secondary_sets_error(
    monkeypatch, caplog, uchrabvr_obj
):
    """
    Готовим данные: две 'primary' строки (18 и 48) и одна 'secondary' (305) для одного сотрудника и месяца.
    Ожидаем: лог ошибки и return_code == 1, выполнение ветки:
        if len(nums_in_person_uchrabvr) > 1: ...
    """
    datan, datok = "2025-01-01", "2025-01-31"
    rows = [
        # две основные (18 и 48 в одной группе primary для 305)
        UchrabvrStructure(
            nrec="10",
            tabn="001",
            mes="01",
            mesn="Январь",
            vidop="18",
            summa="0.00",
            summaval="0.00",
            datan=datan,
            datok=datok,
            clsch="A",
        ),
        UchrabvrStructure(
            nrec="12",
            tabn="001",
            mes="01",
            mesn="Январь",
            vidop="48",
            summa="0.00",
            summaval="0.00",
            datan=datan,
            datok=datok,
            clsch="A",
        ),
        # одна вторичная, которая должна маппиться на ту же группу
        UchrabvrStructure(
            nrec="11",
            tabn="001",
            mes="01",
            mesn="Январь",
            vidop="305",
            summa="0.00",
            summaval="50.00",
            datan=datan,
            datok=datok,
            clsch="A",
        ),
    ]

    # подменяем источник данных; сигнатуру глушим универсально
    monkeypatch.setattr(
        uchrabvr_obj.common, "input_table", lambda *a, **k: (r for r in rows)
    )

    with caplog.at_level(logging.ERROR):
        uchrabvr_obj.start()

    assert uchrabvr_obj.return_code == 1
    assert "имеет больше одного основного вида оплаты" in caplog.text


def test_cli_main_write_failure_sets_return_code_and_exits_1(monkeypatch, tmp_path):
    """
    Ошибка записи файла (PermissionError) -> код выхода 1.
    Глушим service_warning() и input(), чтобы не читать stdin.
    """
    import SRC.uchrabvr as mod

    # не читаем stdin и не показываем предупреждения
    monkeypatch.setattr(mod.Uchrabvr, "service_warning", lambda self: None)
    monkeypatch.setattr(builtins, "input", lambda *a, **k: "")

    # успешные start/stop
    monkeypatch.setattr(mod.Uchrabvr, "start", lambda self: None)
    monkeypatch.setattr(mod.Uchrabvr, "stop", lambda self: None)
    # результат "на запись"
    monkeypatch.setattr(
        mod.Uchrabvr,
        "output_result",
        lambda self: ["UPDATE uchrabvr WHERE nrec=10 SET summa:=100.00;"],
    )
    # не трогаем реальное выключение логирования
    monkeypatch.setattr(mod.logging, "shutdown", lambda: None)

    # перенастроим init, чтобы указать путь вывода внутрь tmp_path
    real_init = mod.Uchrabvr.__init__

    def fake_init(self):
        real_init(self)
        self.parameters["output_file_path"] = str(Path(tmp_path) / "out" / "result.sql")

    monkeypatch.setattr(mod.Uchrabvr, "__init__", fake_init)

    # open() при записи бросает PermissionError
    def fake_open(*a, **k):
        raise PermissionError("no write")

    monkeypatch.setattr(builtins, "open", fake_open)

    with pytest.raises(SystemExit) as se:
        runpy.run_module("SRC.uchrabvr", run_name="__main__")
    assert se.value.code == 1

    def fake_init(self):
        real_init(self)
        # перенаправляем вывод в тестовую папку
        self.parameters["output_file_path"] = str(Path(tmp_path) / "out" / "result.sql")
        return None

    monkeypatch.setattr(mod.Uchrabvr, "__init__", fake_init)

    # open() бросает PermissionError — эмулируем отказ записи
    def fake_open(*a, **k):
        raise PermissionError("no write")

    monkeypatch.setattr(builtins, "open", fake_open)

    with pytest.raises(SystemExit) as se:
        # запустим модуль как скрипт
        runpy.run_module("SRC.uchrabvr", run_name="__main__")

    assert se.value.code == 1
