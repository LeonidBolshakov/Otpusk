from __future__ import annotations
from pathlib import Path
import logging
import sys

# Импортируем из основного модуля
from SRC.uchrabvr import Uchrabvr, TEXT_ERROR
from SRC.common import error

# Важно: кодировка и перевод строки — как в текущем CLI-хвосте,
# чтобы результат совпадал с эталонами byte-в-byte.
_ENC = "cp866"
_NEWLINE = "\r\n"


def run_once() -> int:
    """
    Полный цикл CLI:
      - создать объект
      - start() / stop()
      - записать результат в файл (из parameters["output_file_path"])
      - вернуть код возврата (0 при успехе)
    Без чтения argv — пути берём из cfg.
    """
    app = Uchrabvr()
    try:
        app.start()
    except KeyboardInterrupt:
        error("-----", TEXT_ERROR[7], logging.CRITICAL)
        raise
    except (FileNotFoundError, PermissionError, ValueError):
        return 1

    app.stop()

    out_path = Path(app.parameters_dict["output_file_path"])
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(out_path, "w", encoding=_ENC, newline=_NEWLINE) as f:
            f.write("\n".join(app.output_result()))
    except FileNotFoundError, PermissionError:
        error("-----", TEXT_ERROR[8], logging.CRITICAL)
        app.return_code = 1

    if app.return_code:
        error("-----", TEXT_ERROR[6], logging.CRITICAL)

    return int(app.return_code)


def main() -> None:
    """Entry point для `python -m SRC.uchrabvr_cli` или console_script."""
    sys.exit(run_once())
