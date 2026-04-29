# import importlib
# import json
# import os
# import asyncio
# from abc import ABC, abstractmethod
# from typing import Any, Dict, List

# import openai

# from functions.function_manifest import tools
# from logger_config import get_logger
# from services.call_context import CallContext
# from services.event_emmiter import EventEmitter

# logger = get_logger("LLMService")

# class AbstractLLMService(EventEmitter, ABC):
#     def __init__(self, context: CallContext):
#         super().__init__()
#         self.system_message = context.system_message
#         self.initial_message = context.initial_message
#         self.context = context
#         self.user_context = [
#             {"role": "user", "content": "Hello"},
#             {"role": "assistant", "content": self.initial_message}
#         ]
#         self.partial_response_index = 0
#         self.available_functions = {}

#         for tool in tools:
#             function_name = tool['function']['name']
#             try:
#                 module = importlib.import_module(f'functions.{function_name}')
#                 self.available_functions[function_name] = getattr(module, function_name)
#             except (ImportError, AttributeError) as e:
#                 logger.error(f"Error loading function {function_name}: {e}")

#         self.sentence_buffer = ""
#         context.user_context = self.user_context

#     def set_call_context(self, context: CallContext):
#         self.context = context
#         self.user_context = [
#             {"role": "user", "content": "Hello"},
#             {"role": "assistant", "content": context.initial_message}
#         ]
#         context.user_context = self.user_context
#         self.system_message = context.system_message
#         self.initial_message = context.initial_message

#     @abstractmethod
#     async def completion(self, text: str, interaction_count: int, role: str = 'user', name: str = 'user'):
#         pass

#     def reset(self):
#         self.partial_response_index = 0

#     def validate_function_args(self, args):
#         try:
#             return json.loads(args)
#         except json.JSONDecodeError:
#             logger.warning(f"Invalid function arguments returned by LLM: {args}")
#             return {}

#     def split_into_sentences(self, text):
#         sentences = [''.join(sentences[i:i+2]) for i in range(0, len(sentences), 2)]
#         return sentences

#     async def emit_complete_sentences(self, text, interaction_count):
#         self.sentence_buffer += text
#         sentences = self.split_into_sentences(self.sentence_buffer)

#         for sentence in sentences[:-1]:
#             await self.emit('llmreply', {
#                 "partialResponseIndex": self.partial_response_index,
#                 "partialResponse": sentence.strip()
#             }, interaction_count)
#             self.partial_response_index += 1

#         self.sentence_buffer = sentences[-1] if sentences else ""


# class OpenAIService(AbstractLLMService):
#     def __init__(self, context: CallContext):
#         super().__init__(context)
#         openai.api_key = os.getenv("OPENAI_API_KEY")

#     async def completion(self, text: str, interaction_count: int, role: str = 'user', name: str = 'user'):
#         try:
#             self.user_context.append({"role": role, "content": text, "name": name})

#             # 🔹 **Detect if the user wants to transfer the call**
#             transfer_phrases = ["transfer me", "talk to an agent", "talk to a human", "I need an agent", "transfer", "human", "real person", "transfer the call", "transfer a call"]
#             if any(phrase in text.lower() for phrase in transfer_phrases):
#                 logger.info("User requested a transfer. Initiating transfer...")

#                 # 🔹 Step 1: Send a professional transfer message
#                 transfer_message = "Alright, just a moment. We are transferring your call to an available agent now."
#                 await self.emit('llmreply', {
#                     "partialResponseIndex": None,
#                     "partialResponse": transfer_message
#                 }, interaction_count)

#                 # 🔹 Step 2: Wait for a brief moment before transferring
#                 await asyncio.sleep(2)

#                 # 🔹 Step 3: Initiate the call transfer
#                 if "transfer_call" in self.available_functions:
#                     await self.available_functions["transfer_call"](self.context, {})
#                 return

#             # 🔹 **Detect if the user wants to end the call**
#             exit_phrases = ["bye", "goodbye", "hasta luego", "see you", "exit", "end call"]
#             if any(phrase in text.lower() for phrase in exit_phrases):
#                 logger.info("Detected exit phrase. Sending final message before ending call...")

#                 # 🔹 Step 1: Send a proper farewell response
#                 farewell_message = "Thank you for your time! Have a great day!"
#                 await self.emit('llmreply', {
#                     "partialResponseIndex": None,
#                     "partialResponse": farewell_message
#                 }, interaction_count)

#                 # 🔹 Step 2: Wait 3 seconds before disconnecting
#                 await asyncio.sleep(2)

#                 # 🔹 Step 3: End the call using Twilio
#                 if "end_call" in self.available_functions:
#                     await self.available_functions["end_call"](self.context, {})
#                 return

#             messages = [{"role": "system", "content": self.system_message}] + self.user_context

#             response = openai.ChatCompletion.create(
#                 model="gpt-4",
#                 messages=messages,
#                 stream=True
#             )

#             for chunk in response:
#                 content = chunk.get("choices", [{}])[0].get("delta", {}).get("content", "")
#                 await self.emit_complete_sentences(content, interaction_count)

#         except Exception as e:
#             logger.error(f"Error in OpenAIService completion: {str(e)}")



# class LLMFactory:
#     @staticmethod
#     def get_llm_service(service_name: str, context: CallContext) -> AbstractLLMService:
#         if service_name.lower() == "openai":
#             return OpenAIService(context)
#         else:
#             raise ValueError(f"Unsupported LLM service: {service_name}")


import importlib
import json
import os
import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set
from datetime import datetime, timedelta
from time import monotonic

import openai
import google.generativeai as genai

from functions.function_manifest import tools
from logger_config import get_logger
from services.call_context import CallContext
from services.event_emmiter import EventEmitter
from services.call_context_storage import save_call_context
from services.prompt_repository import read_global_prompt
from services.text_utils import (
    clean_alnum_space,
    extract_words,
    normalize_whitespace,
    remove_comma_before_punct,
    replace_insensitive,
    strip_space_before_punct,
    strip_trailing_word_and_punct,
)

logger = get_logger("LLMService")

class AbstractLLMService(EventEmitter, ABC):
    def __init__(self, context: CallContext):
        super().__init__()
        self.context = context
        self.system_message = self._compose_system_message(context.system_message)
        self.user_context = []
        self.partial_response_index = 0
        self.sentence_buffer = ""
        self._last_emit_ts = monotonic()
        self._min_chunk_chars = int(os.getenv("LLM_MIN_CHARS", "12"))
        self._max_chunk_chars = int(os.getenv("LLM_MAX_CHARS", "120"))
        self._latency_budget = float(os.getenv("LLM_RESPONSE_BUDGET_SEC", "0.12"))
        self._sentence_timeout = float(os.getenv("LLM_SENTENCE_TIMEOUT", "0.06"))
        self._min_sentence_words = int(os.getenv("LLM_MIN_SENTENCE_WORDS", "2"))
        self._enforce_sentence_boundaries = os.getenv(
            "LLM_ENFORCE_SENTENCE_BOUNDARIES", "true"
        ).lower() == "true"
        self._allow_mid_sentence_split = os.getenv(
            "LLM_ALLOW_MID_SENTENCE_SPLIT", "false"
        ).lower() == "true"
        self._force_flush_on_budget = os.getenv(
            "LLM_FORCE_FLUSH_ON_BUDGET", "false"
        ).lower() == "true"
        self._interaction_start: Dict[int, float] = {}
        self._first_response_sent: Set[int] = set()
        self._latency_task: Optional[asyncio.Task] = None
        self.available_functions = {}
        self._max_sentences_per_turn = int(os.getenv("LLM_MAX_SENTENCES_PER_TURN", "0"))
        self._sentences_emitted: Dict[int, int] = {}
        self._turn_capped: Set[int] = set()
        self._max_history_messages = int(os.getenv("LLM_MAX_HISTORY_MESSAGES", "6"))
        self._temperature = float(os.getenv("LLM_TEMPERATURE", "0.3"))
        self._max_tokens = max(200, int(os.getenv("LLM_MAX_TOKENS", "200")))
        self._incomplete_tail_tokens = {
            "or", "and", "but", "to", "for", "with", "in", "of", "on", "at",
            "from", "by", "about", "into", "as", "if", "so", "than", "then",
            "the", "a", "an", "this", "that", "these", "those", "through", "via",
            "your", "our", "my", "new", "next",
            "one", "two", "three", "four", "five", "six",
        }
        self._short_ok_tokens = {
            "yes", "no", "ok", "okay", "sure", "thanks", "thank", "right",
            "great", "fine", "good", "hello", "hi",
        }
        self._filler_prefixes = [
            "okay ",
            "okay",
            "ok ",
            "ok",
            "sure ",
            "sure",
            "alright ",
            "alright",
            "understood ",
            "understood",
            "right ",
            "right,",
            "let me ",
            "let me",
            "now let me ",
            "now let me",
            "now, ",
            "now ",
            "allow me ",
            "allow me",
            "here's ",
            "here is ",
            "here is",
            "i appreciate ",
            "i appreciate your ",
            "i'm going to ",
            "i am going to ",
            "i can ",
            "i will ",
            "now i'll ",
            "now i'll",
            "now i will ",
            "now i will",
        ]
        self._banished_exact = {
            "i appreciate your enthusiasm.",
            "i appreciate your enthusiasm",
            "thank you for your response.",
            "thank you for your response",
            "thank you very much for your time today.",
            "thank you very much for your time today",
            "thank you for your time today.",
            "thank you for your time today",
            "that's great to hear!",
            "that's great to hear",
            "that's wonderful to hear!",
            "that's wonderful to hear",
            "here's the overview.",
            "here's the overview",
            "now let me share the",
        }

        # Load available functions
        for tool in tools:
            function_name = tool['function']['name']
            try:
                module = importlib.import_module(f'functions.{function_name}')
                self.available_functions[function_name] = getattr(module, function_name)
            except (ImportError, AttributeError) as e:
                logger.error(f"Error loading function {function_name}: {e}")

        # Add date calculation utility function
        self.available_functions['calculate_date'] = self.calculate_date

        # Update context with the modified system message
        context.system_message = self.system_message
        context.user_context = self.user_context

        self._default_config = {
            "min_chunk_chars": self._min_chunk_chars,
            "max_chunk_chars": self._max_chunk_chars,
            "latency_budget": self._latency_budget,
            "sentence_timeout": self._sentence_timeout,
            "min_sentence_words": self._min_sentence_words,
            "max_history_messages": self._max_history_messages,
            "temperature": self._temperature,
            "max_tokens": self._max_tokens,
        }

    def _compose_system_message(self, base_message: str) -> str:
        global_prompt = read_global_prompt().strip()
        agent_prompt = (base_message or "").strip()
        prompt_sections = [section for section in (global_prompt, agent_prompt) if section]
        combined_prompt = "\n\n".join(prompt_sections)
        current_date = datetime.now()
        current_date_str = current_date.strftime("%m, %d, %Y")
        conversation_constraints = (
            "Conversation guardrails:\n"
            "- Speak exactly one complete sentence per response. Stop after one sentence and wait.\n"
            "- Keep that single sentence short (ideally under fifteen words).\n"
            "- Avoid long adjective+noun phrases that can be cut mid-stream. Split into two short sentences instead.\n"
            "- Ask one concise question at a time only when you need new information.\n"
            "- Never restart the introduction or re-state campaign details that the caller already acknowledged—remember prior answers.\n"
            "- Start directly with the answer; do not preface with acknowledgments like \"okay\", \"sure\", \"understood\", or \"right\".\n"
            "- Never prepend words with filler noises (no 'uh', 'ahh', 'hmm', throat-clearing, or audible inhaling); start directly with the first word.\n"
            "- No filler noises or mouth sounds during sentences; speak naturally but stay direct.\n"
            "- Begin speaking within ~400‑500 ms of receiving usable speech and maintain a steady cadence with zero dead air.\n"
            "- Stop immediately when the caller talks over you, acknowledge their words, and only resume after their pause.\n"
            "- Stay calm, professional, and confident; enthusiasm is fine but never ramble or stack multiple questions.\n"
            "- Never address the caller using your own name or title. Use the caller's name only if they explicitly provide it.\n"
            "- If a script includes the agent name, treat it as your identity, not the caller's.\n"
            "- Follow the provided script order and wording closely; only deviate to answer a direct question.\n"
            "- Only respond using exact or near-exact lines from the provided agent script.\n"
            "- If the script includes a transfer or callback step, follow it exactly and only at the correct time.\n"
            "- Never say 'How can I assist you today' or 'How can I help you today'—use the script's qualification question instead.\n"
            "- If the script or agent prompt provides prices, sizes, or configuration details, treat them as authoritative and use them verbatim. Never estimate, round, generalize, or invent numbers; if a value is not explicitly stated, say it's not available and offer to connect to a human.\n"
            "- Never ask to schedule a site visit or appointment and never ask for date or time. If the user requests a site visit, offer a transfer to a senior advisor instead.\n"
            "- Never end a response mid-sentence. Always finish the sentence before stopping.\n"
        )
        scheduling_rules = (
            f"Today's date is {current_date_str}. Use this date for any calculations or references "
            f"involving the current date, month, or year.\n"
            f"The 'current week' refers to the week containing today's date, starting from Monday. "
            f"For example, if today is 2025-04-16 (Wednesday), the current week runs from Monday, 2025-04-14, to Sunday, 2025-04-20.\n"
            f"The 'next week' refers to the following week, starting from the next Monday. "
            f"For example, if today is 2025-04-16, the next week runs from Monday, 2025-04-21, to Sunday, 2025-04-27.\n"
            f"When the user mentions 'next [day]' (e.g., 'next Monday'), interpret it as the specified day in the next week. "
            f"For example, if today is 2025-04-16 (Wednesday), 'next Monday' refers to 2025-04-28.\n"
            f"When the user mentions 'coming [day]' or 'this [day]' (e.g., 'coming Monday' or 'this Monday'), interpret it as the specified day in the current week. "
            f"For example, if today is 2025-04-16 (Wednesday), 'coming Monday' refers to 2025-04-14. "
            f"If the specified day in the current week has already passed and the context implies a future event (e.g., scheduling), assume the user means the same day in the next week (e.g., 2025-04-21).\n"
            f"If the user mentions a day without 'next' or 'coming' (e.g., 'Monday'), assume they mean the nearest upcoming day unless context suggests otherwise.\n"
            f"All time references should use the 24-hour format (e.g., '15:00' for 3:00 PM)."
        )
        pronunciation_rules = (
            "Pronounce special characters clearly as follows:\n"
            "Underscore (_) → 'underscore'\n"
            "Hyphen (-) → 'dash'\n"
            "Dot (.) → 'dot'\n"
            "At (@) → 'at'\n"
            "Forward Slash (/) → 'slash'\n"
            "Backslash (\\) → 'backslash'\n"
            "Colon (:) → 'colon'\n"
            "Semicolon (;) → 'semicolon'\n"
            "Comma (,) → 'comma'\n"
            "Hash (#) → 'hash' or 'number'\n"
            "Percent (%) → 'percent'\n"
            "Asterisk (*) → 'asterisk'\n"
            "Ampersand (&) → 'and'\n"
            "Plus (+) → 'plus'\n"
            "Equal (=) → 'equals'"
        )
        sections = []
        if combined_prompt:
            sections.append(combined_prompt)
        sections.append(conversation_constraints)
        sections.extend([scheduling_rules, pronunciation_rules])
        return "\n\n".join(section for section in sections if section.strip())

    def _overview_remainder(self, reply: str) -> str:
        if not reply:
            return ""
        overview = getattr(self.context, "project_overview_text", None) or ""
        if not overview:
            return ""
        if reply.rstrip().endswith((".", "!", "?")):
            return ""
        reply_words = [clean_alnum_space(w) for w in reply.split()]
        reply_words = [w for w in reply_words if w]
        if not reply_words:
            return ""
        # Only attempt to stitch if this looks like the overview.
        anchor_terms = {
            "project", "acres", "towers", "clubhouse", "gardens", "retail",
            "frontage", "landscapes", "lifestyle", "community", "magarpatta",
        }
        if not any(word in anchor_terms for word in reply_words):
            return ""
        overview_words = overview.split()
        overview_norm = [clean_alnum_space(w) for w in overview_words]
        overview_norm = [w for w in overview_norm if w]
        if not overview_norm:
            return ""
        # Subsequence match: find last overview word that appears in reply in order.
        i = 0
        j = 0
        last_match = -1
        matches = 0
        while i < len(overview_norm) and j < len(reply_words):
            if overview_norm[i] == reply_words[j]:
                last_match = i
                matches += 1
                i += 1
                j += 1
            else:
                j += 1
        if matches < 4 or last_match >= len(overview_words) - 1:
            return ""
        remainder_words = overview_words[last_match + 1:]
        remainder = " ".join(remainder_words).strip()
        return remainder

    @staticmethod
    def _format_price_table(context: CallContext) -> str:
        if context is None:
            return ""
        raw = getattr(context, "pricing_data", None)
        if not raw:
            return ""

        def _iter_entries(data):
            entries = []
            if isinstance(data, dict):
                for bhk_key, value in data.items():
                    values = value if isinstance(value, list) else [value]
                    for item in values:
                        if isinstance(item, dict):
                            entry = dict(item)
                            entry.setdefault("bhk", bhk_key)
                            entries.append(entry)
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        entries.append(dict(item))
            return entries

        lines = ["PRICE_TABLE (authoritative; quote verbatim):"]
        for entry in _iter_entries(raw):
            label = str(entry.get("label") or "").strip()
            bhk = str(entry.get("bhk") or "").strip()
            size = str(entry.get("size") or "").strip()
            price = str(entry.get("price") or "").strip()
            if not label and bhk:
                label = f"{bhk} BHK"
            parts = []
            if price:
                parts.append(f"price {price}")
            if size:
                parts.append(f"size {size}")
            if not parts:
                continue
            line = f"- {label}: " + ", ".join(parts)
            lines.append(line)

        if len(lines) == 1:
            return ""
        return "\n".join(lines)

    def _strip_directives(self, text: str) -> str:
        if not text:
            return ""
        marker = "[Response directives]"
        if marker in text:
            text = text.split(marker, 1)[0]
        return text.strip()

    def set_call_context(self, context: CallContext):
        self.context = context
        self.user_context = []
        self.system_message = self._compose_system_message(context.system_message)
        context.system_message = self.system_message
        context.user_context = self.user_context
        self._last_emit_ts = monotonic()
        self.partial_response_index = 0
        self.sentence_buffer = ""

    @abstractmethod
    async def completion(
        self,
        text: str,
        interaction_count: int,
        role: str = 'user',
        name: str = 'user',
        raw_text: Optional[str] = None,
    ):
        pass

    def reset(self):
        self.partial_response_index = 0
        self.sentence_buffer = ""
        self._last_emit_ts = monotonic()
        self._cancel_latency_task()
        self._interaction_start.clear()
        self._first_response_sent.clear()
        self._sentences_emitted.clear()
        self._turn_capped.clear()

    def _cancel_latency_task(self):
        task = self._latency_task
        if task:
            task.cancel()
        self._latency_task = None

    def _prime_latency_timer(self, interaction_count: int):
        if self._latency_budget <= 0:
            return
        if self._latency_task:
            return
        if not self.sentence_buffer.strip():
            return
        self._latency_task = asyncio.create_task(self._latency_timeout(interaction_count))

    async def _latency_timeout(self, interaction_count: int):
        task = self._latency_task
        try:
            await asyncio.sleep(self._latency_budget)
            if not self.sentence_buffer.strip():
                return
            if self._enforce_sentence_boundaries and not (
                self._allow_mid_sentence_split or self._force_flush_on_budget
            ):
                logger.debug(
                    "Latency budget reached; sentence boundaries enforced, holding buffer."
                )
                return
            buffered_text = self.sentence_buffer.strip()
            if len(buffered_text) < self._min_chunk_chars and not self._allow_mid_sentence_split:
                return
            self.sentence_buffer = ""
            if buffered_text and buffered_text[-1] not in ".?!":
                buffered_text = f"{buffered_text}."
            if not self._is_safe_emit_fragment(buffered_text):
                return
            logger.debug(f"Latency budget reached; forcing emit for interaction {interaction_count}.")
            await self._emit_sentence(buffered_text, interaction_count)
        except asyncio.CancelledError:
            pass
        finally:
            if self._latency_task is task:
                self._latency_task = None

    def _start_interaction_latency(self, interaction_count: int):
        self._cancel_latency_task()
        self._interaction_start[interaction_count] = monotonic()
        self._first_response_sent.discard(interaction_count)
        self._sentences_emitted[interaction_count] = 0
        self._turn_capped.discard(interaction_count)

    def _record_first_response(self, interaction_count: int):
        if interaction_count in self._first_response_sent:
            return
        start = self._interaction_start.pop(interaction_count, None)
        if start is None:
            return
        latency_ms = (monotonic() - start) * 1000
        target_ms = self._latency_budget * 1000
        if latency_ms > target_ms:
            logger.warning(f"Interaction {interaction_count} first audio chunk at {latency_ms:.0f}ms (> target {target_ms:.0f}ms)")
        else:
            logger.info(f"Interaction {interaction_count} first audio chunk at {latency_ms:.0f}ms (target {target_ms:.0f}ms)")
        self._first_response_sent.add(interaction_count)

    def validate_function_args(self, args):
        try:
            return json.loads(args)
        except json.JSONDecodeError:
            logger.warning(f"Invalid function arguments returned by LLM: {args}")
            return {}

    def _is_decimal_point(self, text: str, idx: int) -> bool:
        if idx <= 0 or idx >= len(text) - 1:
            return False
        prev_char = text[idx - 1]
        if not prev_char.isdigit():
            return False
        next_idx = idx + 1
        while next_idx < len(text) and text[next_idx].isspace():
            next_idx += 1
        if next_idx >= len(text):
            # Treat trailing "1." as a decimal fragment until we see the next digit.
            return True
        if text[next_idx].isdigit():
            return True
        return False

    def _is_inline_abbreviation(self, text: str, idx: int) -> bool:
        if idx <= 0 or idx >= len(text) - 1:
            return False
        prev_char = text[idx - 1]
        next_idx = idx + 1
        while next_idx < len(text) and text[next_idx].isspace():
            next_idx += 1
        if next_idx >= len(text):
            return False
        next_char = text[next_idx]
        # Patterns such as "sq.ft" or "e.g." should not trigger a sentence split.
        return (
            prev_char.isalpha()
            and next_char.isalpha()
            and prev_char.islower()
            and next_char.islower()
        )

    def _is_numeric_inline(self, text: str, idx: int) -> bool:
        """
        Handle cases like "915. square feet" where we want the entire phrase to stay
        together even though there's a literal period after the digits.
        """
        if idx <= 0 or idx >= len(text) - 1:
            return False
        prev_char = text[idx - 1]
        if not prev_char.isdigit():
            return False
        next_idx = idx + 1
        while next_idx < len(text) and text[next_idx].isspace():
            next_idx += 1
        if next_idx >= len(text):
            return False
        next_char = text[next_idx]
        return next_char.islower()

    def split_into_sentences(self, text: str) -> List[str]:
        sentences: List[str] = []
        start = 0
        length = len(text)
        i = 0
        while i < length:
            ch = text[i]
            if ch in ".?!":
                if ch == ".":
                    if self._is_decimal_point(text, i) or self._is_inline_abbreviation(text, i):
                        i += 1
                        continue
                    if self._is_numeric_inline(text, i):
                        i += 1
                        continue
                boundary = i + 1
                sentences.append(text[start:boundary])
                start = boundary
            i += 1
        if start < length:
            sentences.append(text[start:])
        return sentences

    def _extract_sentence_fragment(self, text: str) -> (str, str):
        """Return the longest substring ending with sentence punctuation and the remainder."""
        for delimiter in ".?!":
            idx = text.rfind(delimiter)
            while idx != -1:
                if delimiter == "." and (
                    self._is_decimal_point(text, idx)
                    or self._is_inline_abbreviation(text, idx)
                    or self._is_numeric_inline(text, idx)
                ):
                    idx = text.rfind(delimiter, 0, idx)
                    continue
                end_idx = idx + 1
                return text[:end_idx].strip(), text[end_idx:].lstrip()
            break
        return "", text

    def _sanitize_sentence(self, sentence: str) -> str:
        text = (sentence or "").strip()
        if not text:
            return ""
        lower = text.lower()
        if lower in self._banished_exact:
            return ""
        for prefix in self._filler_prefixes:
            if lower.startswith(prefix):
                text = text[len(prefix):].lstrip(" ,.-!")
                lower = text.lower()
                if not text:
                    return ""
                if lower in self._banished_exact:
                    return ""
                break
        if lower.startswith(("and ", "or ", "but ")):
            parts = text.split(None, 1)
            if len(parts) < 2:
                return ""
            joiner = parts[0].lower()
            remainder = parts[1].strip()
            if not remainder:
                return ""
            if joiner == "and":
                text = f"Also, {remainder}"
            elif joiner == "or":
                text = f"Alternatively, {remainder}"
            else:
                text = f"However, {remainder}"
        text = replace_insensitive(text, "sq ft", "square feet")
        text = replace_insensitive(text, "sq. ft", "square feet")
        text = replace_insensitive(text, "sqft", "square feet")
        text = replace_insensitive(text, "square foot", "square feet")
        text = normalize_whitespace(text)
        text = remove_comma_before_punct(text)
        text = strip_space_before_punct(text)
        text = text.rstrip(" ,;:")
        lower = text.lower()
        lower_trim = lower.rstrip(" .!?")
        if lower_trim.endswith("buying or"):
            idx = lower_trim.rfind("buying or")
            text = (text[:idx] + "buying or investing in a new home or project").strip()
        return text

    def _is_safe_emit_fragment(self, text: str) -> bool:
        """Guard against emitting mid-thought fragments."""
        snippet = (text or "").strip()
        if not snippet:
            return False
        words = [w.lower() for w in extract_words(snippet, allow_apostrophe=True)]
        if len(words) < self._min_sentence_words:
            if len(words) == 1 and words[0] in self._short_ok_tokens:
                return True
            return False
        last = words[-1] if words else ""
        if last in self._incomplete_tail_tokens:
            return False
        if snippet.endswith((",", ";", ":")):
            return False
        return True

    def _starts_with_joiner(self, text: str) -> bool:
        words = [w.lower() for w in extract_words(text or "", allow_apostrophe=True)]
        if not words:
            return False
        return words[0] in {"and", "or", "but"}

    def _starts_with_bhk_tail(self, text: str) -> bool:
        words = [w.lower() for w in extract_words(text or "", allow_apostrophe=True)]
        if not words:
            return False
        return words[0] == "bhk"

    def _ends_with_bhk_fragment(self, text: str) -> bool:
        cleaned = (text or "").strip().lower()
        if not cleaned:
            return False
        cleaned = cleaned.rstrip(" .,!?:;")
        return cleaned.endswith("bhk")

    def _ends_with_incomplete_tail(self, text: str) -> bool:
        words = [w.lower() for w in extract_words(text or "", allow_apostrophe=True)]
        if not words:
            return False
        return words[-1] in self._incomplete_tail_tokens

    @staticmethod
    def _strip_terminal_punct(text: str) -> str:
        return (text or "").rstrip().rstrip(".!?")

    def _trim_incomplete_tail(self, text: str) -> str:
        cleaned = (text or "").strip()
        if not cleaned:
            return ""
        words = [w.lower() for w in extract_words(cleaned, allow_apostrophe=True)]
        if not words:
            return cleaned
        tail = words[-1]
        if tail not in self._incomplete_tail_tokens:
            return cleaned
        trimmed = strip_trailing_word_and_punct(cleaned, tail).strip()
        return trimmed

    def _is_turn_capped(self, interaction_count: int) -> bool:
        return interaction_count in self._turn_capped

    async def _emit_sentence(self, sentence: str, interaction_count: int) -> bool:
        if self._max_sentences_per_turn > 0:
            emitted = self._sentences_emitted.get(interaction_count, 0)
            if emitted >= self._max_sentences_per_turn:
                self._turn_capped.add(interaction_count)
                self.sentence_buffer = ""
                self._cancel_latency_task()
                return False
        cleaned = self._sanitize_sentence(sentence)
        if not cleaned:
            return True
        self._cancel_latency_task()
        self._record_first_response(interaction_count)
        await self.emit('llmreply', {
            "partialResponseIndex": self.partial_response_index,
            "partialResponse": cleaned
        }, interaction_count)
        self.partial_response_index += 1
        self._last_emit_ts = monotonic()
        emitted = self._sentences_emitted.get(interaction_count, 0) + 1
        self._sentences_emitted[interaction_count] = emitted
        if self._max_sentences_per_turn > 0 and emitted >= self._max_sentences_per_turn:
            self._turn_capped.add(interaction_count)
        return True

    async def emit_complete_sentences(self, text, interaction_count) -> List[str]:
        emitted: List[str] = []
        self.sentence_buffer += text
        if text.strip():
            self._prime_latency_timer(interaction_count)
        sentences = self.split_into_sentences(self.sentence_buffer)
        pending_prefix = ""

        for idx, sentence in enumerate(sentences[:-1]):
            candidate_raw = f"{pending_prefix}{sentence}".strip()
            pending_prefix = ""
            if not candidate_raw:
                continue
            next_sentence = sentences[idx + 1] if idx + 1 < len(sentences) else ""
            next_stripped = next_sentence.lstrip()
            if (
                next_stripped
                and next_stripped[0].islower()
                and self._ends_with_bhk_fragment(candidate_raw)
            ):
                pending_prefix = f"{self._strip_terminal_punct(candidate_raw)} "
                continue
            if next_sentence and (
                self._starts_with_joiner(next_sentence)
                or self._starts_with_bhk_tail(next_sentence)
            ):
                pending_prefix = f"{self._strip_terminal_punct(candidate_raw)} "
                continue
            cleaned_candidate = self._sanitize_sentence(candidate_raw)
            if not cleaned_candidate:
                continue
            if self._ends_with_incomplete_tail(cleaned_candidate):
                trimmed = self._trim_incomplete_tail(candidate_raw)
                if trimmed:
                    pending_prefix = f"{self._strip_terminal_punct(trimmed)} "
                continue
            if self._enforce_sentence_boundaries and not self._is_safe_emit_fragment(cleaned_candidate):
                pending_prefix = f"{self._strip_terminal_punct(candidate_raw)} "
                continue
            success = await self._emit_sentence(cleaned_candidate, interaction_count)
            if not success:
                return emitted
            emitted.append(cleaned_candidate.strip())

        self.sentence_buffer = f"{pending_prefix}{sentences[-1] if sentences else ''}"
        tail = self.sentence_buffer.strip()
        if not tail:
            self._cancel_latency_task()
            return emitted

        self._prime_latency_timer(interaction_count)
        over_length = len(tail) >= self._max_chunk_chars

        emit_text = ""
        remainder = tail

        fragment, remainder_candidate = self._extract_sentence_fragment(tail)
        has_boundary = bool(fragment)

        if has_boundary:
            emit_text, remainder = fragment, remainder_candidate
        elif over_length and self._allow_mid_sentence_split:
            boundary = tail.rfind(" ", 0, self._max_chunk_chars)
            if boundary == -1 or boundary < self._min_chunk_chars:
                boundary = self._max_chunk_chars
            emit_text = tail[:boundary].strip()
            remainder = tail[boundary:].lstrip()

        if not emit_text:
            return emitted

        cleaned_emit = self._sanitize_sentence(emit_text)
        if not cleaned_emit:
            self.sentence_buffer = remainder
            return emitted

        if self._ends_with_incomplete_tail(cleaned_emit):
            cleaned_emit = self._trim_incomplete_tail(cleaned_emit)
            if not cleaned_emit:
                self.sentence_buffer = remainder
                return emitted
        if self._enforce_sentence_boundaries and not self._is_safe_emit_fragment(cleaned_emit):
            self.sentence_buffer = tail
            return emitted

        if cleaned_emit[-1] not in ".?!":
            cleaned_emit = f"{cleaned_emit}."

        success = await self._emit_sentence(cleaned_emit, interaction_count)
        if not success:
            self.sentence_buffer = ""
            return emitted
        emitted.append(cleaned_emit.strip())
        self.sentence_buffer = remainder

        return emitted

    async def flush_sentence_buffer(self, interaction_count: int) -> List[str]:
        if self._is_turn_capped(interaction_count):
            self.sentence_buffer = ""
            self._cancel_latency_task()
            return []
        tail = self.sentence_buffer.strip()
        emitted: List[str] = []
        if tail:
            cleaned_tail = self._sanitize_sentence(tail)
            if not cleaned_tail:
                self.sentence_buffer = ""
                self._cancel_latency_task()
                return []
            if self._ends_with_incomplete_tail(cleaned_tail):
                cleaned_tail = self._trim_incomplete_tail(cleaned_tail)
                if not cleaned_tail:
                    self.sentence_buffer = ""
                    self._cancel_latency_task()
                    return []
            if self._enforce_sentence_boundaries and not self._is_safe_emit_fragment(cleaned_tail):
                self.sentence_buffer = ""
                self._cancel_latency_task()
                return []
            if cleaned_tail[-1] not in ".?!":
                cleaned_tail = f"{cleaned_tail}."
            success = await self._emit_sentence(cleaned_tail, interaction_count)
            if success:
                emitted.append(cleaned_tail)
            self.sentence_buffer = ""
        self._cancel_latency_task()
        return emitted

    async def _handle_transfer_or_exit(self, text: str, interaction_count: int):
        """Common handler for transfer/exit logic"""
        transfer_confirm_phrases = [
            "connect me", "transfer me", "please connect",
            "please transfer", "go ahead and connect", "go ahead and transfer",
            "connect me to an agent", "connect me to a person",
            "talk to an agent", "speak to an agent", "advisor please",
            "transfer my call", "connect me now", "transfer me now",
        ]
        transfer_negative_phrases = [
            "no", "nope", "not now", "don't", "do not", "later",
            "stop", "don't connect", "do not connect"
        ]
        transfer_request_keywords = (
            "transfer", "connect", "agent", "advisor", "representative",
            "consultant", "human", "real person", "live agent", "customer service",
        )
        transfer_patterns = [
            "transfer me",
            "transfer my call",
            "connect me to",
            "connect me to an agent",
            "connect me to a human",
            "connect me to a representative",
            "connect me to an advisor",
            "connect me to a consultant",
            "talk to",
            "speak to",
            "live agent",
            "real person",
            "customer service",
            # Swedish transfer phrases
            "koppla mig vidare",
            "jag vill prata med en person",
            "jag vill prata med en agent",
            "kundservice tack",
        ]


        exit_phrases = [
            # English exit phrases
            "bye", "goodbye", "end call",
            "that's all, thanks", "I'm done",
            "exit conversation", "close chat",
            "no more help needed",
            
            # Swedish exit phrases
            "hej då", "adjö", "avsluta samtal",
            "det var allt, tack", "jag är klar",
            "avsluta konversation", "stäng chatten",
            "behöver ingen mer hjälp"
        ]

        raw_text = self._strip_directives(text)
        text_lower = raw_text.lower()
        if not text_lower:
            return False

        exit_confirm_ttl = float(os.getenv("EXIT_CONFIRM_TTL_SEC", "12"))
        early_exit_grace = float(os.getenv("EARLY_EXIT_GRACE_SEC", "8"))
        exit_pending = bool(getattr(self.context, "exit_confirm_pending", False))
        exit_expires_at = getattr(self.context, "exit_confirm_expires_at", None)
        if exit_pending and exit_expires_at and time.time() > float(exit_expires_at):
            exit_pending = False
            setattr(self.context, "exit_confirm_pending", False)
            setattr(self.context, "exit_confirm_expires_at", None)
            if getattr(self.context, "call_sid", None):
                save_call_context(self.context.call_sid, self.context)
        if exit_pending and not any(phrase in text_lower for phrase in exit_phrases):
            setattr(self.context, "exit_confirm_pending", False)
            setattr(self.context, "exit_confirm_expires_at", None)
            if getattr(self.context, "call_sid", None):
                save_call_context(self.context.call_sid, self.context)
            exit_pending = False

        offer_pending = bool(getattr(self.context, "transfer_offer_pending", False))
        offer_expires_at = getattr(self.context, "transfer_offer_expires_at", None)
        now = time.time()
        if offer_pending and offer_expires_at and now > float(offer_expires_at):
            offer_pending = False
            setattr(self.context, "transfer_offer_pending", False)
            setattr(self.context, "transfer_offer_expires_at", None)
            if getattr(self.context, "call_sid", None):
                save_call_context(self.context.call_sid, self.context)

        if offer_pending:
            if any(phrase in text_lower for phrase in transfer_negative_phrases):
                setattr(self.context, "transfer_offer_pending", False)
                setattr(self.context, "transfer_offer_expires_at", None)
                if getattr(self.context, "call_sid", None):
                    save_call_context(self.context.call_sid, self.context)
                self._cancel_latency_task()
                self._record_first_response(interaction_count)
                await self.emit('llmreply', {
                    "partialResponseIndex": None,
                    "partialResponse": "No problem. I can continue here."
                }, interaction_count)
                return True
            if any(phrase in text_lower for phrase in transfer_confirm_phrases):
                setattr(self.context, "transfer_offer_pending", False)
                setattr(self.context, "transfer_offer_expires_at", None)
                setattr(self.context, "transfer_user_confirmed", True)
                if getattr(self.context, "call_sid", None):
                    save_call_context(self.context.call_sid, self.context)
                self._cancel_latency_task()
                self._record_first_response(interaction_count)
                await self.emit('llmreply', {
                    "partialResponseIndex": None,
                    "partialResponse": "Great. Connecting you to a senior advisor now."
                }, interaction_count)
                await asyncio.sleep(0.6)
                if "transfer_call" in self.available_functions:
                    result = await self.available_functions["transfer_call"](self.context, {})
                    logger.info("Transfer call result: {}", result)
                return True

        explicit_request = (
            any(keyword in text_lower for keyword in transfer_request_keywords)
            and any(pattern in text_lower for pattern in transfer_patterns)
        )
        if explicit_request:
            logger.info("Transfer requested explicitly")
            self._cancel_latency_task()
            self._record_first_response(interaction_count)
            setattr(self.context, "transfer_offer_pending", False)
            setattr(self.context, "transfer_offer_expires_at", None)
            setattr(self.context, "transfer_user_confirmed", True)
            if getattr(self.context, "call_sid", None):
                save_call_context(self.context.call_sid, self.context)
            await self.emit('llmreply', {
                "partialResponseIndex": None,
                "partialResponse": "Great. Connecting you to a senior advisor now."
            }, interaction_count)
            await asyncio.sleep(0.6)
            if "transfer_call" in self.available_functions:
                result = await self.available_functions["transfer_call"](self.context, {})
                logger.info("Transfer call result: {}", result)
            return True
        
        if any(phrase in text_lower for phrase in exit_phrases):
            start_ts = getattr(self.context, "call_start_ts", None)
            early_exit = False
            if start_ts is not None:
                try:
                    early_exit = (time.monotonic() - float(start_ts)) < early_exit_grace
                except Exception:
                    early_exit = False
            if exit_pending:
                setattr(self.context, "exit_confirm_pending", False)
                setattr(self.context, "exit_confirm_expires_at", None)
                if getattr(self.context, "call_sid", None):
                    save_call_context(self.context.call_sid, self.context)
            elif early_exit:
                setattr(self.context, "exit_confirm_pending", True)
                setattr(self.context, "exit_confirm_expires_at", time.time() + exit_confirm_ttl)
                if getattr(self.context, "call_sid", None):
                    save_call_context(self.context.call_sid, self.context)
                self._cancel_latency_task()
                self._record_first_response(interaction_count)
                await self.emit('llmreply', {
                    "partialResponseIndex": None,
                    "partialResponse": "Just to confirm, would you like to end the call?"
                }, interaction_count)
                return True
            logger.info("Exit requested")
            self._cancel_latency_task()
            self._record_first_response(interaction_count)
            await self.emit('llmreply', {
                "partialResponseIndex": None,
                "partialResponse": "Thank you for contacting us!"
            }, interaction_count)
            await asyncio.sleep(1)
            if "end_call" in self.available_functions:
                await self.available_functions["end_call"](self.context, {})
            return True
            
        return False

    def calculate_date(self, context: CallContext, args: Dict[str, Any]) -> str:
        """
        Utility function to calculate a specific date based on user input like 'next Monday' or 'coming Monday'.
        Args:
            context: CallContext object
            args: Dictionary containing 'phrase' (e.g., 'next Monday', 'coming Monday') and optional 'future_intent' (boolean)
        Returns:
            A string representing the calculated date in 'MM, DD, YYYY' format.
        """
        phrase = args.get('phrase', '').lower()
        future_intent = args.get('future_intent', True)  # Default to assuming future events (e.g., scheduling)
        
        today = datetime.now()
        weekday_map = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }

        # Extract day from phrase
        target_day = None
        for token in extract_words(phrase, allow_apostrophe=False):
            if token in weekday_map:
                target_day = token
                break
        if not target_day:
            return "Invalid day specified."
        target_weekday = weekday_map[target_day]
        current_weekday = today.weekday()

        if 'next' in phrase:
            # Next week: Move to the next week's instance of the target day
            days_until_target = (target_weekday - current_weekday + 7) % 7
            if days_until_target == 0:
                days_until_target = 7  # Ensure we move to the next week
            days_until_target += 7  # Add another week to ensure next week
        elif 'coming' in phrase or 'this' in phrase:
            # Current week: Find the target day in the current week
            days_until_target = (target_weekday - current_weekday + 7) % 7
            # If the day has passed and future intent is implied, move to next week
            if days_until_target != 0 and days_until_target < 7 and future_intent:
                days_until_target += 7
        else:
            # Default: Assume nearest upcoming day
            days_until_target = (target_weekday - current_weekday + 7) % 7
            if days_until_target == 0 and future_intent:
                days_until_target = 7  # If today is the target day, assume next week for future intent

        target_date = today + timedelta(days=days_until_target)
        return target_date.strftime("%m, %d, %Y")

class OpenAIService(AbstractLLMService):
    def __init__(self, context: CallContext):
        super().__init__(context)
        openai.api_key = os.getenv("OPENAI_API_KEY")

    async def completion(
        self,
        text: str,
        interaction_count: int,
        role: str = 'user',
        name: str = 'user',
        raw_text: Optional[str] = None,
    ):
        try:
            if getattr(self.context, "transfer_in_progress", False) or getattr(self.context, "transfer_completed", False):
                logger.info("Transfer active; skipping LLM response.")
                return
            self._start_interaction_latency(interaction_count)
            self.user_context.append({"role": role, "content": text, "name": name})

            transfer_text = raw_text if raw_text is not None else text
            if await self._handle_transfer_or_exit(transfer_text, interaction_count):
                return

            history = self.user_context
            if self._max_history_messages > 0:
                history = self.user_context[-self._max_history_messages:]
            messages = [{"role": "system", "content": self.system_message}] + history

            emitted_total: List[str] = []
            model_name = os.getenv("OPENAI_MODEL", "gpt-5-mini")
            token_param = "max_tokens"
            lower_model = (model_name or "").lower()
            restricted_temp = lower_model.startswith(("o1", "o3", "gpt-5", "gpt-4.1"))
            if restricted_temp:
                token_param = "max_completion_tokens"
            if hasattr(openai.ChatCompletion, "acreate"):
                response = await openai.ChatCompletion.acreate(
                    model=model_name,
                    messages=messages,
                    stream=True,
                    **({} if restricted_temp else {"temperature": self._temperature}),
                    **({token_param: self._max_tokens} if self._max_tokens is not None else {}),
                )
                try:
                    async for chunk in response:
                        content = chunk.get("choices", [{}])[0].get("delta", {}).get("content", "")
                        if not content:
                            continue
                        emitted_total.extend(await self.emit_complete_sentences(content, interaction_count))
                        if self._is_turn_capped(interaction_count):
                            break
                finally:
                    # openai==0.28 streaming leaves aiohttp connections open if the generator isn't closed.
                    aclose = getattr(response, "aclose", None)
                    if callable(aclose):
                        try:
                            await aclose()
                        except Exception:
                            pass
            else:
                full_response = await asyncio.to_thread(
                    openai.ChatCompletion.create,
                    model=model_name,
                    messages=messages,
                    stream=False,
                    **({} if restricted_temp else {"temperature": self._temperature}),
                    **({token_param: self._max_tokens} if self._max_tokens is not None else {}),
                )
                content = (full_response.choices[0].message.get("content") or "").strip()
                if content:
                    emitted_total.extend(await self.emit_complete_sentences(content, interaction_count))

            pending_tail = self.sentence_buffer.strip()
            candidate_reply = " ".join([*emitted_total, pending_tail]).strip()
            remainder = self._overview_remainder(candidate_reply)
            if remainder:
                # Ensure we can finish the overview sentence even if the turn was capped.
                if self._is_turn_capped(interaction_count):
                    self._turn_capped.discard(interaction_count)
                if pending_tail:
                    self.sentence_buffer = f"{pending_tail} {remainder}".strip()
                else:
                    self.sentence_buffer = remainder.strip()
            if not self._is_turn_capped(interaction_count):
                emitted_total.extend(await self.flush_sentence_buffer(interaction_count))
            else:
                self.sentence_buffer = ""

            assistant_reply = " ".join(emitted_total).strip()
            if assistant_reply:
                self.user_context.append({
                    "role": "assistant",
                    "content": assistant_reply
                })
                self.context.user_context = self.user_context

        except Exception as e:
            logger.error(f"OpenAI Error: {str(e)}")
            await self.emit('llmerror', {"error": str(e)}, interaction_count)

class GeminiService(AbstractLLMService):
    def __init__(self, context: CallContext):
        super().__init__(context)
        genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
        self.model = genai.GenerativeModel('gemini-2.0-flash')  # Use appropriate model

    async def completion(
        self,
        text: str,
        interaction_count: int,
        role: str = 'user',
        name: str = 'user',
        raw_text: Optional[str] = None,
    ):
        try:
            if getattr(self.context, "transfer_in_progress", False) or getattr(self.context, "transfer_completed", False):
                logger.info("Transfer active; skipping LLM response.")
                return
            self._start_interaction_latency(interaction_count)
            self.user_context.append({"role": role, "content": text, "name": name})

            transfer_text = raw_text if raw_text is not None else text
            if await self._handle_transfer_or_exit(transfer_text, interaction_count):
                return

            messages = []
            messages.append({
                "role": "user",
                "parts": [self.system_message]
            })
            messages.append({
                "role": "model",
                "parts": ["Understood"]
            })
            
            history = self.user_context
            if self._max_history_messages > 0:
                history = self.user_context[-self._max_history_messages:]

            for msg in history:
                if msg["role"] == "user":
                    messages.append({
                        "role": "user",
                        "parts": [msg["content"]]
                    })
                elif msg["role"] == "assistant":
                    messages.append({
                        "role": "model",
                        "parts": [msg["content"]]
                    })

            response = await self.model.generate_content_async(messages, stream=True)

            full_response = ""
            async for chunk in response:
                if chunk.text:
                    full_response += chunk.text
                    await self.emit_complete_sentences(chunk.text, interaction_count)
                    if self._is_turn_capped(interaction_count):
                        break

            pending_tail = self.sentence_buffer.strip()
            candidate_reply = full_response.strip()
            remainder = self._overview_remainder(candidate_reply)
            if remainder:
                if self._is_turn_capped(interaction_count):
                    self._turn_capped.discard(interaction_count)
                if pending_tail:
                    self.sentence_buffer = f"{pending_tail} {remainder}".strip()
                else:
                    self.sentence_buffer = remainder.strip()
                full_response = f"{full_response} {remainder}".strip()
            if not self._is_turn_capped(interaction_count):
                await self.flush_sentence_buffer(interaction_count)
            else:
                self.sentence_buffer = ""

            if full_response:
                self.user_context.append({
                    "role": "assistant",
                    "content": full_response
                })
                if hasattr(self.context, 'call_sid'):
                    save_call_context(self.context.call_sid, self.context)

        except Exception as e:
            logger.error(f"Gemini Error: {str(e)}")
            await self.emit('llmerror', {"error": str(e)}, interaction_count)

class LLMFactory:
    @staticmethod
    def get_llm_service(service_name: str, context: CallContext) -> AbstractLLMService:
        service_name = service_name.lower()
        if service_name == "openai":
            return OpenAIService(context)
        elif service_name in ["gemini", "google-gemini"]:
            return GeminiService(context)
        else:
            raise ValueError(f"Unsupported LLM service: {service_name}")
