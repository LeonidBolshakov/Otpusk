from __future__ import (
    annotations,
)  # чтобы не ругался на типы, которые ещё не импортированы
from pathlib import Path
import sys
import pytest
from _pytest.config import Config, Parser

ROOT = Path(__file__).resolve().parents[1]  # C:\2_otpusk
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ===== Pytest CLI options =====
def pytest_addoption(parser: Parser) -> None:
    """
    Добавляет параметры командной строки:
      --uchrabvr-data      : путь к папке с входными файлами (UCHRABVR.txt, uchrabvr.cfg)
      --uchrabvr-expected  : путь к папке с эталонными результатами
    """
    base: Path = Path(__file__).resolve().parent  # папка, где лежит этот файл
    parser.addoption(
        "--uchrabvr-data",
        default=str(base),  # путь по умолчанию
        help="Папка с UCHRABVR.txt и uchrabvr.cfg (по умолчанию: рядом с тестами)",
    )
    parser.addoption(
        "--uchrabvr-expected",
        default=str(base),
        help="Папка с эталонами: uchrabvr_update.lot, uchrabvr.log (по умолчанию: рядом с тестами)",
    )


@pytest.fixture(scope="session")
def data_dir(pytestconfig: Config) -> Path:
    """Возвращает путь к каталогу с входными файлами."""
    return Path(pytestconfig.getoption("--uchrabvr-data")).resolve()


@pytest.fixture(scope="session")
def expected_dir(pytestconfig: Config) -> Path:
    """Возвращает путь к каталогу с эталонными файлами."""
    return Path(pytestconfig.getoption("--uchrabvr-expected")).resolve()
