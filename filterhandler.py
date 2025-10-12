import logging


class FilteringHandler(logging.Handler):
    """Фильтрует записи перед передачей целевой обработчик"""

    def __init__(self, target: logging.Handler):
        super().__init__()
        self.target = target

    def emit(self, record: logging.LogRecord) -> None:
        if not "*** | ***" in record.getMessage():
            self.target.emit(record)
