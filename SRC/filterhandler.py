"""
Модуль filterhandler
--------------------
Автор: Л. А. Большаков, 2025

Назначение:
    FilteringHandler — промежуточный обработчик, который отфильтровывает записи
    с заданным маркером `service_text` и передаёт остальные в целевой обработчик.

Сценарий:
    logger.addHandler(FilteringHandler(file_handler, service_text="[SERVICE]"))

Особенности:
    • Уровень и формат берутся у целевого обработчика.
    • Если сообщение содержит `service_text`, запись отбрасывается.

Пример использования:
    >>> import logging
    >>> from SRC.filterhandler import FilteringHandler

    >>> # Базовый обработчик, например, файл
    >>> file_handler = logging.FileHandler("app.log")
    >>> file_handler.setLevel(logging.INFO)

    >>> # Создаём фильтрующий обработчик, исключающий служебные сообщения
    >>> filter_handler = FilteringHandler(file_handler, service_text="[SERVICE]")

    >>> logger = logging.getLogger("example")
    >>> logger.addHandler(filter_handler)
    >>> logger.setLevel(logging.INFO)

    >>> logger.info("Normal message")        # попадёт в файл
    >>> logger.info("Something [SERVICE]")   # будет отфильтровано
"""

import logging
import sys


class FilteringHandler(logging.Handler):
    """
    Фильтрует сообщения по ключевому тексту перед передачей в `target`.

    Атрибуты:
        target       (logging.Handler): Целевой обработчик для «чистых» записей
        service_text (str): Строка-маркер; если содержится в сообщении,
        запись отбрасывается.
    """

    def __init__(self, target: logging.Handler, service_text: str) -> None:
        """
        Инициализирует фильтрующий обработчик.

        Args:
            target (logging.Handler): Целевой обработчик для передачи записей
            service_text (str): Ключевой текст, при наличии которого запись будет исключена
        """
        super().__init__()
        self.target = target
        self.service_text = service_text

        # Синхронизируем уровень и формат с целевым обработчиком
        self.setLevel(target.level)
        self.setFormatter(target.formatter)

    def emit(self, record: logging.LogRecord) -> None:
        """
        Обрабатывает лог-запись: проверяет, содержит ли сообщение `service_text`.
        Если нет — передаёт запись в целевой обработчик (`target`).

        Args:
            record (logging.LogRecord): Лог-запись, сформированная логгером.
        """
        # Проверяем, содержит ли сообщение служебный текст
        if self.service_text not in record.getMessage():
            try:
                # Передаём запись дальше по цепочке
                self.target.handle(record)
            except (FileNotFoundError, PermissionError) as e:
                # В случае ошибок доступа к файлу — выводим понятное сообщение
                print(
                    f"Ошибка доступа к файлу журнала логирования "
                    f"\n(возможно, неверно указан путь или нет прав):"
                    f"\n{e}",
                    file=sys.stderr,
                )
