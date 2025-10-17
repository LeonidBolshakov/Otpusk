"""
Модуль accumulatevidops
-----------------------
Автор: Л. А. Большаков, 2025

Описание:
    Модуль содержит класс AccumulateVidops, предназначенный для накопления
    уникальных vidop из сообщений системы логирования Python.
    Класс можно использовать как пользовательский
    обработчик (Handler) для стандартного модуля logging.

    Принцип работы:
    - Класс принимает строку `service_text`, которая служит маркером-разделителем.
    - При получении лог-сообщения он ищет в нём этот маркер.
    - Предполагается, что после маркера следует vidop. Он извлекается и сохраняется в множестве.
    - Таким образом, накапливается набор уникальных vidop-значений.

Пример использования:
    >>> import logging
    >>> logger = logging.getLogger()
    >>> handler = AccumulateVidops(service_text="VIDOP:")
    >>> logger.addHandler(handler)
    >>> logger.warning("System event VIDOP:1234")
    >>> logger.warning("System event VIDOP:5678")
    >>> print(handler.output_accumulate())
    {'1234', '5678'}
"""

import logging


# noinspection SpellCheckingInspection
class AccumulateVidops(logging.Handler):
    """
    Класс-обработчик для накопления уникальных vidop-значений из логов.

    Атрибуты:
        service_text (str): Ключевая строка, по которой производится поиск
            нужных данных в тексте логов
        accumulate (set[str]): Множество для хранения найденных уникальных vidop-значений.

    Методы:
        emit(record): Обрабатывает лог-запись, извлекая vidop-значение, если
            сообщение содержит указанный `service_text`
        output_accumulate(): Возвращает множество накопленных vidop-значений.
    """

    def __init__(self, service_text: str) -> None:
        """
        Инициализирует обработчик.

        Args:
            service_text (str): Текст-маркер, используемый для поиска vidop в сообщениях.
        """
        super().__init__()
        self.service_text = service_text
        # Используем множество для исключения дубликатов vidop-значений
        self.accumulate: set[str] = set()

    def emit(self, record: logging.LogRecord) -> None:
        """
        Обрабатывает запись лога и сохраняет найденное vidop-значение.

        Args:
            record (logging.LogRecord): Объект лог-записи.

        Принцип:
            Если в сообщении содержится `service_text`,
            то извлекается часть строки после него,
            очищается от пробелов и добавляется в множество.
            На дальнейшую обработку сообщение не отправляется.
        """
        message = record.getMessage()
        if self.service_text in message:
            # Извлекаем часть строки после service_text
            vidop = message.split(self.service_text)[1]
            # Добавляем очищенное значение в множество
            self.accumulate.add(vidop.strip())

    def output_accumulate(self) -> set[str]:
        """
        Возвращает множество всех накопленных vidop-значений.

        Returns:
            Уникальные vidop-значения, найденные в логах.
        """
        return self.accumulate
