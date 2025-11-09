import re
import shutil
import logging
from pathlib import Path

# ===== Импорты тестируемого кода =====
from SRC.uchrabvr import Uchrabvr
from SRC.uchrabvr_cli import run_once

# ===== Утилиты сравнения =====
_TS = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:,\d{3})?")
_PID = re.compile(r"\(pid=\d+\)")
_SPACES = re.compile(r"[ \t]+$")  # хвостовые пробелы


def _read_text(path: Path, enc: str, fallback: str | None = None) -> str:
    try:
        return path.read_text(encoding=enc)
    except UnicodeDecodeError:
        if fallback:
            return path.read_text(encoding=fallback, errors="ignore")
        raise


def _normalize_log(s: str, roots: list[Path]) -> str:
    # 1) выравниваем перевод строк
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    # 2) убираем timestamp и PID (если пишутся)
    s = _TS.sub("<TS>", s)
    s = _PID.sub("(pid=<PID>)", s)
    # 3) нормализуем абсолютные пути (tmp/каталог) → <ROOT>
    for root in sorted(roots, key=lambda p: len(str(p)), reverse=True):
        s = s.replace(str(root), "<ROOT>")
    # 4) чистим хвостовые пробелы
    s = "\n".join(_SPACES.sub("", line) for line in s.split("\n"))
    return s.strip() + "\n"


def _normalize_lot(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = "\n".join(_SPACES.sub("", line) for line in s.split("\n"))
    return s.strip() + "\n"


# ===== Сам тест =====
def test_e2e_real_files(
    tmp_path: Path, data_dir: Path, expected_dir: Path, monkeypatch, caplog
):
    # 1) Подготовим рабочую папку: копируем реальные вход и cfg
    work = tmp_path
    for name in ("UCHRABVR.txt", "uchrabvr.cfg"):
        src = data_dir / name
        assert src.exists(), f"Не найден входной файл: {src}"
        shutil.copy2(src, work / name)

    # 2) Чтобы программа писала ровно в нужные имена — работаем из этой папки
    monkeypatch.chdir(work)

    # 3) Глушим паузу и подсказку
    monkeypatch.setattr(Uchrabvr, "service_warning", lambda self: None)

    # 4) Запускаем полный цикл
    caplog.clear()
    with caplog.at_level(logging.INFO):
        rc = run_once()
        assert rc == 1

    # 5) Читаем фактические результаты
    lot_actual = work / "uchrabvr_update.lot"
    log_actual = work / "uchrabvr.log"
    assert lot_actual.exists(), "Не создан uchrabvr_update.lot"
    assert log_actual.exists(), "Не создан uchrabvr.log"

    # 6) Читаем эталоны
    lot_expected = expected_dir / "uchrabvr_update.lot"
    log_expected = expected_dir / "uchrabvr.log"
    assert lot_expected.exists(), f"Нет эталона: {lot_expected}"
    assert log_expected.exists(), f"Нет эталона: {log_expected}"

    # 7) Нормализуем и сравниваем
    #    LOT обычно в cp866; LOG — в UTF-8 (подхватим fallback на всякий)
    lot_a = _normalize_lot(_read_text(lot_actual, enc="cp866", fallback="utf-8"))
    lot_e = _normalize_lot(_read_text(lot_expected, enc="cp866", fallback="utf-8"))
    assert lot_a == lot_e, "Содержимое .lot отличается от эталона"

    roots = [work, data_dir, expected_dir]
    log_a = _normalize_log(
        _read_text(log_actual, enc="utf-8", fallback="cp1251"), roots
    )
    # print(f"{log_a!r}")
    log_e = _normalize_log(
        _read_text(log_expected, enc="utf-8", fallback="cp1251"), roots
    )
    assert log_a == log_e, "Лог отличается от эталона"
