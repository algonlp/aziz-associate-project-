from typing import List


WORD_BOUNDARY_EXTRA = "_"


def normalize_whitespace(text: str) -> str:
    return " ".join((text or "").split())


def strip_non_digits(text: str) -> str:
    return "".join(ch for ch in (text or "") if ch.isdigit())


def clean_alnum_space(text: str) -> str:
    if not text:
        return ""
    out = []
    for ch in text.lower():
        if ch.isalnum() or ch.isspace():
            out.append(ch)
        else:
            out.append(" ")
    return normalize_whitespace("".join(out))


def clean_alnum_space_remove_apostrophes(text: str) -> str:
    if not text:
        return ""
    out = []
    for ch in text.lower():
        if ch in ("'", "\u2019", "\u2018"):
            continue
        if ch.isalnum() or ch.isspace():
            out.append(ch)
        else:
            out.append(" ")
    return normalize_whitespace("".join(out))


def extract_words(text: str, allow_apostrophe: bool = True, allow_dash: bool = False) -> List[str]:
    words: List[str] = []
    buf: List[str] = []
    for ch in text or "":
        if ch.isalpha() or (allow_apostrophe and ch in ("'", "\u2019")) or (allow_dash and ch == "-"):
            buf.append(ch)
        else:
            if buf:
                words.append("".join(buf))
                buf = []
    if buf:
        words.append("".join(buf))
    return words


def extract_numbers(text: str, allow_decimal: bool = True) -> List[str]:
    numbers: List[str] = []
    buf: List[str] = []
    seen_dot = False
    for ch in text or "":
        if ch.isdigit():
            buf.append(ch)
        elif allow_decimal and ch == "." and buf and not seen_dot:
            buf.append(ch)
            seen_dot = True
        else:
            if buf:
                numbers.append("".join(buf))
                buf = []
                seen_dot = False
            else:
                seen_dot = False
    if buf:
        numbers.append("".join(buf))
    return numbers


def replace_insensitive(text: str, target: str, replacement: str) -> str:
    if not text or not target:
        return text or ""
    lower = text.lower()
    target_lower = target.lower()
    out: List[str] = []
    i = 0
    tlen = len(target)
    while True:
        idx = lower.find(target_lower, i)
        if idx == -1:
            out.append(text[i:])
            break
        out.append(text[i:idx])
        out.append(replacement)
        i = idx + tlen
    return "".join(out)


def strip_space_before_punct(text: str, punct: str = ".,?!;:") -> str:
    if not text:
        return ""
    out: List[str] = []
    pending_space = False
    for ch in text:
        if ch == " ":
            pending_space = True
            continue
        if pending_space:
            if ch in punct:
                out.append(ch)
            else:
                out.append(" ")
                out.append(ch)
            pending_space = False
        else:
            out.append(ch)
    if pending_space:
        out.append(" ")
    return "".join(out)


def remove_comma_before_punct(text: str, punct: str = ".?!") -> str:
    if not text:
        return ""
    out: List[str] = []
    i = 0
    length = len(text)
    while i < length:
        ch = text[i]
        if ch == ",":
            j = i + 1
            while j < length and text[j].isspace():
                j += 1
            if j < length and text[j] in punct:
                i = j
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def strip_leading_symbols(text: str, symbols: str = "-\u2013\u2014\u2022") -> str:
    if not text:
        return ""
    i = 0
    length = len(text)
    while i < length and (text[i].isspace() or text[i] in symbols):
        i += 1
    return text[i:]


def starts_with_word(text: str, word: str) -> bool:
    if not text or not word:
        return False
    stripped = text.lstrip()
    lower = stripped.lower()
    word_lower = word.lower()
    if not lower.startswith(word_lower):
        return False
    idx = len(word)
    if idx >= len(stripped):
        return True
    after = stripped[idx]
    return not (after.isalnum() or after == WORD_BOUNDARY_EXTRA)


def find_word_index(text: str, word: str) -> int:
    if not text or not word:
        return -1
    lower = text.lower()
    word_lower = word.lower()
    start = 0
    while True:
        idx = lower.find(word_lower, start)
        if idx == -1:
            return -1
        before = text[idx - 1] if idx > 0 else ""
        after_index = idx + len(word)
        after = text[after_index] if after_index < len(text) else ""
        if (not before or not (before.isalnum() or before == WORD_BOUNDARY_EXTRA)) and (
            not after or not (after.isalnum() or after == WORD_BOUNDARY_EXTRA)
        ):
            return idx
        start = idx + len(word)


def split_on_first_dash(text: str) -> List[str]:
    if not text:
        return [""]
    dash_positions = [pos for pos in (text.find("-"), text.find("\u2013")) if pos != -1]
    if not dash_positions:
        return [text]
    idx = min(dash_positions)
    return [text[:idx], text[idx + 1 :]]


def has_digit(text: str) -> bool:
    return any(ch.isdigit() for ch in text or "")


def is_numbered_list_item(text: str) -> bool:
    if not text:
        return False
    stripped = text.lstrip()
    if not stripped:
        return False
    digits = []
    i = 0
    while i < len(stripped) and stripped[i].isdigit():
        digits.append(stripped[i])
        i += 1
    if not digits:
        return False
    return i < len(stripped) and stripped[i] == "."


def is_all_caps_heading(text: str) -> bool:
    if not text:
        return False
    stripped = text.strip()
    if len(stripped) < 4:
        return False
    if not stripped[0].isupper():
        return False
    for ch in stripped:
        if not (ch.isupper() or ch in " &/-"):
            return False
    return True


def contains_any_token(text: str, tokens: List[str]) -> bool:
    lower = (text or "").lower()
    return any(token in lower for token in tokens)


def strip_trailing_word_and_punct(text: str, tail: str) -> str:
    if not text or not tail:
        return text or ""
    lower = text.lower()
    tail_lower = tail.lower()
    idx = lower.rfind(tail_lower)
    if idx == -1:
        return text
    after_idx = idx + len(tail)
    if after_idx < len(text):
        after = text[after_idx:]
        if any(ch.isalnum() or ch == WORD_BOUNDARY_EXTRA for ch in after):
            return text
    before = text[idx - 1] if idx > 0 else ""
    if before and (before.isalnum() or before == WORD_BOUNDARY_EXTRA):
        return text
    trimmed = text[:idx].rstrip()
    while trimmed and trimmed[-1] in " .,:;!?":
        trimmed = trimmed[:-1]
        trimmed = trimmed.rstrip()
    return trimmed


def lower_strip(text: str) -> str:
    return (text or "").strip().lower()
