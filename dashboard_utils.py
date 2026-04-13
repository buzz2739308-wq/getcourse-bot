"""Вспомогательные утилиты для dashboard-скриптов."""
import re

_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


def sanitize(value):
    """Убирает управляющие символы из значений, идущих в Google Sheets.

    Числа и None пропускает без изменений. Строки прогоняет через UTF-8
    с ignore и вычищает control-символы (\\x00..\\x1f, \\x7f).
    """
    if value is None or isinstance(value, (int, float, bool)):
        return value
    s = str(value).encode("utf-8", "ignore").decode("utf-8")
    return _CONTROL_RE.sub("", s)


def sanitize_counts(counts: dict) -> dict:
    """Применяет sanitize ко всем значениям dict (ключи — имена каналов)."""
    return {k: sanitize(v) for k, v in counts.items()}
