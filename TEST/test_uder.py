import sys, pathlib

sys.path.insert(
    0, str(pathlib.Path(__file__).resolve().parent.parent)
)  # -> C:\2_otpusk

import logging
import pytest

from SRC.uder import Uder, UderStructure


@pytest.fixture
def uder_obj(tmp_path):
    obj = Uder()
    obj.parameters["file_log_path"] = str(tmp_path / "uder.log")
    obj.common.init_logging()
    return obj


def test_normalize_mount():
    u = Uder()
    assert u.normalize_mount("") == "00"
    assert u.normalize_mount("7") == "07"
    assert u.normalize_mount("11") == "11"
    assert u.normalize_mount("12XX") == "12"


def test_grouping_and_info_logged(monkeypatch, caplog, uder_obj):
    clsch = "EMP1"
    rows = [
        UderStructure(
            nrec="1",
            tabn="001",
            mes="5",
            vidud="13",
            sumud="100.00",
            clsch=clsch,
            datav="2025-05-31",
            vidoplud="X",
        ),
        UderStructure(
            nrec="2",
            tabn="001",
            mes="05",
            vidud="13",
            sumud="-90.00",
            clsch=clsch,
            datav="2025-05-31",
            vidoplud="X",
        ),
        UderStructure(
            nrec="3",
            tabn="001",
            mes="7",
            vidud="13",
            sumud="999.00",
            clsch=clsch,
            datav="2025-07-31",
            vidoplud="X",
        ),
    ]
    monkeypatch.setattr(
        uder_obj.common, "input_table", lambda file, Table: (r for r in rows)
    )
    with caplog.at_level(logging.INFO):
        uder_obj.start()

    messages = [rec.getMessage() for rec in caplog.records]
    assert any("Разница сумм налогов" in m and "; 10.00" in m for m in messages)


def test_filter_sort_by_group(uder_obj):
    clsch = "E"
    uder_obj.person_uders = [
        UderStructure(
            nrec="1",
            tabn="T",
            mes="6",
            vidud="13",
            sumud="1.00",
            clsch=clsch,
            datav="",
            vidoplud="",
        ),
        UderStructure(
            nrec="2",
            tabn="T",
            mes="4",
            vidud="182",
            sumud="2.00",
            clsch=clsch,
            datav="",
            vidoplud="",
        ),
        UderStructure(
            nrec="3",
            tabn="T",
            mes="5",
            vidud="999",
            sumud="3.00",
            clsch=clsch,
            datav="",
            vidoplud="",
        ),
    ]
    uder_obj._normalize_data()
    filtered = uder_obj.filter_sort_by_group()
    assert [f.group_vidud for f in filtered] == ["04", "06"]
