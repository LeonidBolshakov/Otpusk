"""
Модуль accumulatevidops
-----------------------
Автор: Л. А. Большаков, 2025

Назначение:
    Класс AccumulateVidops накапливает уникальные значения «vidop» из сообщений
    стандартного модуля logging. Используется как пользовательский обработчик (Handler).

Как работает:
    • В сообщении ищется маркер `service_text`.
    • Всё, что расположено после маркера, трактуется как «vidop», приводится к str.strip()
      и сохраняется во множестве `accumulate`.
    • Обработчик не передаёт запись дальше по цепочке (терминальная ветка).

Ограничения:
    • Извлечение выполняется по первому вхождению `service_text`; если маркер встречается
      несколько раз, учитывается часть строки после первого вхождения.
    • «Vidop» берётся «как есть» до конца сообщения; если нужно обрезать по разделителям
      (пробел, «;», «,» и пр.), эту логику следует добавить здесь.

Пример:
    >>> import logging
    >>> h = AccumulateVidops(service_text="VIDOP:")
    >>> root = logging.getLogger()
    >>> root.addHandler(h)
    >>> root.warning("System event VIDOP:1234")
    >>> root.warning("System event VIDOP:5678")
    >>> h.output_accumulate()
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
        self.accumulate: set[str] = set()

    def emit(self, record: logging.LogRecord) -> None:
        """
        Обрабатывает запись лога: если `service_text` найден, извлекает часть
        после маркера, выполняет strip() и добавляет в множество.

        Примечание:
            Запись не передаётся дальше по цепочке обработчиков.
        """
        message = record.getMessage()
        if self.service_text and self.service_text in message:
            # Извлекаем часть строки после service_text
            _, _, vidop = message.partition(self.service_text)
            # Добавляем очищенное значение в множество
            self.accumulate.add(vidop.strip())

    def output_accumulate(self) -> set[str]:
        """Возвращает множество всех накопленных vidop-значений."""
        return self.accumulate
