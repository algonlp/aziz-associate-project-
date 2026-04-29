import os
from pathlib import Path
from typing import Optional


PROMPT_FILE_PATH = os.getenv(
    "GLOBAL_SYSTEM_PROMPT_FILE",
    os.path.join(os.getcwd(), "system_prompt.txt"),
)


def _prompt_file() -> Path:
    path = Path(PROMPT_FILE_PATH)
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    return path


def read_global_prompt() -> str:
    """
    Return the globally configured system prompt. Prefer the persisted prompt file,
    falling back to the SYSTEM_MESSAGE environment variable for backwards compatibility.
    """
    path = Path(PROMPT_FILE_PATH)
    if path.exists():
        try:
            return path.read_text(encoding="utf-8")
        except OSError:
            pass

    env_prompt = os.getenv("SYSTEM_MESSAGE", "")
    if env_prompt and not path.exists():
        try:
            _prompt_file().write_text(env_prompt, encoding="utf-8")
        except OSError:
            pass
    return env_prompt


def write_global_prompt(prompt: Optional[str]) -> None:
    """
    Persist the new global system prompt and update the in-memory environment so
    future os.getenv calls (if any remain) also see the latest value.
    """
    text = prompt or ""
    path = _prompt_file()
    path.write_text(text, encoding="utf-8")
    os.environ["SYSTEM_MESSAGE"] = text
