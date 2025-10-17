"""
Модуль filterhandler
--------------------
Автор: Л. А. Большаков, 2025

Описание:
    Этот модуль содержит класс `FilteringHandler`, предназначенный для
    фильтрации сообщений логов перед их передачей целевому обработчику (`target`).

    Класс используется как промежуточный фильтр в цепочке логирования Python.
    Он перехватывает записи (`LogRecord`) и передаёт их дальше только в том случае,
    если сообщение **не содержит** заданный текстовый маркер `service_text`.

    Это может быть полезно, если необходимо исключить из логов служебные записи
    (например, сообщения, используемые другими обработчиками или системами мониторинга).

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


class FilteringHandler(logging.Handler):
    """
    Обработчик логов, фильтрующий сообщения по ключевому тексту,
    прежде чем передавать их целевому обработчику.

    Атрибуты:
        target (logging.Handler): Целевой обработчик, который получает записи,
            не содержащие `service_text`.
        Service_text (str): Строка, по наличию которой сообщение фильтруется
            (если содержится в сообщении, оно пропускается).
    """

    def __init__(self, target: logging.Handler, service_text: str) -> None:
        """
        Инициализирует фильтрующий обработчик.

        Args:
            target (logging.Handler): Целевой обработчик для передачи записей. S
            service_text (str): Ключевой текст, при наличии которого запись будет исключена.
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
                    f"\n{e}"
                )
