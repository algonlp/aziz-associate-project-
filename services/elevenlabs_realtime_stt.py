import asyncio
import base64
import math
import os
import re
import time
from array import array
from typing import AsyncGenerator, List, Optional, Tuple

from elevenlabs.realtime.connection import RealtimeConnection, RealtimeEvents
from elevenlabs.realtime.scribe import AudioFormat, CommitStrategy, RealtimeAudioOptions, ScribeRealtime

from logger_config import get_logger
from pipecat.frames.frames import InterimTranscriptionFrame, TranscriptionFrame, Language
from pipecat.services.stt_service import STTService
from pipecat.utils.time import time_now_iso8601

logger = get_logger("ElevenLabsRealtimeSTT")


class ElevenLabsRealtimeSTTService(STTService):
    def __init__(
        self,
        *,
        api_key: str,
        sample_rate: Optional[int] = None,
        language: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(sample_rate=sample_rate, **kwargs)
        self._scribe = ScribeRealtime(api_key=api_key)
        self._connection: Optional[RealtimeConnection] = None
        self._connect_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._session_id: Optional[str] = None
        self._connected = False

        self._language = self._normalize_language(language or "en")
        self._audio_format = self._resolve_audio_format(
            os.getenv("ELEVENLABS_STT_AUDIO_FORMAT", "")
        )
        self._commit_strategy = self._resolve_commit_strategy(
            os.getenv("ELEVENLABS_STT_COMMIT_STRATEGY", "vad")
        )
        self._vad_silence_threshold = float(
            os.getenv("ELEVENLABS_STT_VAD_SILENCE_THRESHOLD", "0.55")
        )
        self._vad_threshold = float(os.getenv("ELEVENLABS_STT_VAD_THRESHOLD", "0.45"))
        self._min_speech_ms = int(os.getenv("ELEVENLABS_STT_MIN_SPEECH_MS", "80"))
        self._min_silence_ms = int(os.getenv("ELEVENLABS_STT_MIN_SILENCE_MS", "80"))

        self._chunk_ms = int(os.getenv("ELEVENLABS_STT_CHUNK_MS", "80"))
        self._pcm_buffer = bytearray()
        self._chunk_bytes = 0

        self._pending_partial = ""
        self._flush_task: Optional[asyncio.Task] = None
        self._flush_delay = float(os.getenv("TRANSCRIPT_FLUSH_SEC", "0.05"))
        self._noise_gate_enabled = os.getenv("STT_NOISE_GATE_ENABLED", "false").lower() == "true"
        self._noise_gate_dbfs = float(os.getenv("STT_NOISE_GATE_DBFS", "-50"))
        self._noise_gate_hold_ms = int(os.getenv("STT_NOISE_GATE_HOLD_MS", "250"))
        self._noise_gate_max_silence_sec = float(
            os.getenv("STT_NOISE_GATE_MAX_SILENCE_SEC", "0.8")
        )
        self._gate_open_until = 0.0
        self._last_audio_sent_ts = 0.0
        self._startup_bypass_sec = float(os.getenv("STT_STARTUP_BYPASS_SEC", "0.0"))
        self._connect_ts = 0.0
        self._hpf_enabled = os.getenv("STT_HPF_ENABLED", "false").lower() == "true"
        self._hpf_cutoff_hz = float(os.getenv("STT_HPF_CUTOFF_HZ", "80"))
        self._hpf_prev_x = 0.0
        self._hpf_prev_y = 0.0
        self._soft_gate_enabled = os.getenv("STT_SOFT_GATE_ENABLED", "false").lower() == "true"
        self._soft_gate_dbfs = float(os.getenv("STT_SOFT_GATE_DBFS", "-45"))
        self._soft_gate_attenuation = float(os.getenv("STT_SOFT_GATE_ATTENUATION", "0.2"))
        self._prime_silence_sec = float(os.getenv("STT_PRIME_WITH_SILENCE_SEC", "2.0"))
        self._primed = False
        self._text_replacements = self._parse_text_replacements(
            os.getenv("STT_TEXT_REPLACEMENTS", "")
        )

    async def start(self, frame):
        await super().start(frame)
        if not self._audio_format:
            self._audio_format = self._format_for_rate(self.sample_rate)
        self._chunk_bytes = max(2, int(self.sample_rate * self._chunk_ms / 1000) * 2)
        # Eager-connect so early user speech (human-first) doesn't wait on STT setup.
        await self._ensure_connected()

    async def stop(self, frame):
        await self._disconnect()

    async def set_language(self, language: Language):
        self._language = self._normalize_language(getattr(language, "value", None) or str(language))

    async def run_stt(self, audio: bytes) -> AsyncGenerator[None, None]:
        if not audio:
            yield None
            return
        await self._ensure_connected()
        self._pcm_buffer.extend(audio)
        while len(self._pcm_buffer) >= self._chunk_bytes:
            chunk = bytes(self._pcm_buffer[:self._chunk_bytes])
            del self._pcm_buffer[:self._chunk_bytes]
            await self._send_pcm_chunk(chunk)
        yield None

    async def _ensure_connected(self):
        if self._connected and self._connection:
            return
        async with self._connect_lock:
            if self._connected and self._connection:
                return
            await self._connect()

    async def _connect(self):
        options: RealtimeAudioOptions = {
            "model_id": os.getenv("ELEVENLABS_STT_MODEL_ID", "scribe_v2_realtime"),
            "audio_format": self._audio_format,
            "sample_rate": self.sample_rate,
            "commit_strategy": self._commit_strategy,
            "language_code": self._language,
        }

        if self._commit_strategy == CommitStrategy.VAD:
            options["vad_silence_threshold_secs"] = self._vad_silence_threshold
            options["vad_threshold"] = self._vad_threshold
            options["min_speech_duration_ms"] = self._min_speech_ms
            options["min_silence_duration_ms"] = self._min_silence_ms

        logger.debug(
            "Connecting ElevenLabs realtime STT (model={}, format={}, rate={}, strategy={}, lang={})",
            options["model_id"],
            self._audio_format.value,
            self.sample_rate,
            self._commit_strategy.value,
            self._language,
        )

        connection = await self._scribe.connect(options)
        self._connect_ts = time.monotonic()
        connection.on(RealtimeEvents.PARTIAL_TRANSCRIPT, self._handle_partial)
        connection.on(RealtimeEvents.COMMITTED_TRANSCRIPT, self._handle_final)
        connection.on(RealtimeEvents.COMMITTED_TRANSCRIPT_WITH_TIMESTAMPS, self._handle_final)
        connection.on(RealtimeEvents.ERROR, self._handle_error)
        connection.on(RealtimeEvents.CLOSE, self._handle_close)
        connection.on(RealtimeEvents.SESSION_STARTED, self._handle_session_started)

        self._connection = connection
        self._connected = True
        await self._prime_with_silence()

    async def _disconnect(self):
        async with self._connect_lock:
            if not self._connection:
                return
            try:
                await self._flush_pending(force=True)
                await self._connection.close()
            except Exception:
                pass
            finally:
                self._connection = None
                self._connected = False
                self._pending_partial = ""
                self._cancel_flush_task()

    async def _send_pcm_chunk(self, chunk: bytes):
        if not chunk or not self._connection:
            return
        if self._hpf_enabled:
            chunk = self._apply_hpf(chunk)
        now = time.monotonic()
        startup_bypass = self._startup_bypass_sec > 0 and self._connect_ts and (now - self._connect_ts) < self._startup_bypass_sec
        if not startup_bypass:
            if self._soft_gate_enabled:
                chunk = self._apply_soft_gate(chunk)
            if self._noise_gate_enabled:
                if self._is_below_noise_gate(chunk):
                    if now >= self._gate_open_until:
                        if self._last_audio_sent_ts and (now - self._last_audio_sent_ts) < self._noise_gate_max_silence_sec:
                            return
                        # Send explicit silence to keep the stream alive without triggering STT output.
                        chunk = b"\x00" * len(chunk)
                    # else gate is open; allow audio through
                else:
                    self._gate_open_until = now + (self._noise_gate_hold_ms / 1000.0)
        payload = base64.b64encode(chunk).decode("utf-8")
        async with self._send_lock:
            await self._connection.send(
                {"audio_base_64": payload, "sample_rate": self.sample_rate}
            )
            self._last_audio_sent_ts = time.monotonic()

    async def _prime_with_silence(self) -> None:
        if self._primed or not self._connection:
            return
        if self._prime_silence_sec <= 0:
            return
        # Ensure chunk size is initialized
        if not self._chunk_bytes:
            self._chunk_bytes = max(2, int(self.sample_rate * self._chunk_ms / 1000) * 2)
        total_bytes = int(self.sample_rate * self._prime_silence_sec) * 2
        chunk = b"\x00" * min(self._chunk_bytes, total_bytes)
        sent = 0
        try:
            while sent < total_bytes:
                payload = base64.b64encode(chunk).decode("utf-8")
                await self._connection.send(
                    {"audio_base_64": payload, "sample_rate": self.sample_rate}
                )
                sent += len(chunk)
            self._primed = True
        except Exception:
            return

    def _handle_partial(self, data):
        transcript = self._extract_transcript_text(data)
        if not transcript:
            return
        transcript = self._apply_text_replacements(transcript)
        if not transcript:
            return
        logger.debug("ElevenLabs partial transcript: {}", transcript)
        self._pending_partial = transcript
        self._schedule_flush()
        asyncio.create_task(
            self.push_frame(
                InterimTranscriptionFrame(
                    transcript,
                    self._user_id,
                    time_now_iso8601(),
                    self._language_enum(),
                    result=data,
                )
            )
        )

    def _handle_final(self, data):
        transcript = self._extract_transcript_text(data)
        if not transcript:
            return
        transcript = self._apply_text_replacements(transcript)
        if not transcript:
            return
        logger.debug("ElevenLabs final transcript: {}", transcript)
        self._pending_partial = ""
        self._cancel_flush_task()
        asyncio.create_task(
            self.push_frame(
                TranscriptionFrame(
                    transcript,
                    self._user_id,
                    time_now_iso8601(),
                    self._language_enum(),
                    result=data,
                )
            )
        )

    def _handle_error(self, data):
        logger.error("ElevenLabs realtime STT error: {}", data)
        self._connected = False

    def _handle_close(self, *_):
        logger.info("ElevenLabs realtime STT connection closed.")
        self._connected = False
        self._connection = None
        self._pending_partial = ""
        self._cancel_flush_task()

    def _handle_session_started(self, data):
        session_id = data.get("session_id")
        if session_id:
            self._session_id = session_id
            logger.debug("ElevenLabs STT session ready (id={})", session_id)

    def _is_below_noise_gate(self, chunk: bytes) -> bool:
        try:
            samples = array("h")
            samples.frombytes(chunk)
            if not samples:
                return True
            sumsq = 0
            for s in samples:
                sumsq += s * s
            rms = math.sqrt(sumsq / len(samples))
            if rms <= 0:
                return True
            dbfs = 20 * math.log10(rms / 32768.0)
            return dbfs < self._noise_gate_dbfs
        except Exception:
            return False

    def _apply_hpf(self, chunk: bytes) -> bytes:
        try:
            samples = array("h")
            samples.frombytes(chunk)
            if not samples:
                return chunk
            rc = 1.0 / (2.0 * math.pi * max(1.0, self._hpf_cutoff_hz))
            dt = 1.0 / float(self.sample_rate or 16000)
            alpha = rc / (rc + dt)
            prev_x = self._hpf_prev_x
            prev_y = self._hpf_prev_y
            for i in range(len(samples)):
                x = float(samples[i])
                y = alpha * (prev_y + x - prev_x)
                prev_x = x
                prev_y = y
                samples[i] = int(max(-32768, min(32767, y)))
            self._hpf_prev_x = prev_x
            self._hpf_prev_y = prev_y
            return samples.tobytes()
        except Exception:
            return chunk

    def _apply_soft_gate(self, chunk: bytes) -> bytes:
        try:
            samples = array("h")
            samples.frombytes(chunk)
            if not samples:
                return chunk
            sumsq = 0
            for s in samples:
                sumsq += s * s
            rms = math.sqrt(sumsq / len(samples)) if samples else 0.0
            if rms <= 0:
                return chunk
            dbfs = 20 * math.log10(rms / 32768.0)
            if dbfs >= self._soft_gate_dbfs:
                return chunk
            attenuation = max(0.0, min(1.0, self._soft_gate_attenuation))
            for i in range(len(samples)):
                samples[i] = int(samples[i] * attenuation)
            return samples.tobytes()
        except Exception:
            return chunk

    def _schedule_flush(self):
        if self._flush_task:
            return
        self._flush_task = asyncio.create_task(self._flush_after_delay())

    def _cancel_flush_task(self):
        if self._flush_task:
            self._flush_task.cancel()
            self._flush_task = None

    async def _flush_after_delay(self):
        try:
            await asyncio.sleep(self._flush_delay)
            await self._flush_pending(force=False)
        except asyncio.CancelledError:
            pass
        finally:
            self._flush_task = None

    async def _flush_pending(self, *, force: bool):
        if not self._pending_partial:
            return
        text = self._apply_text_replacements(self._pending_partial)
        if not text:
            self._pending_partial = ""
            return
        if force:
            self._pending_partial = ""
        await self.push_frame(
            TranscriptionFrame(
                text,
                self._user_id,
                time_now_iso8601(),
                self._language_enum(),
                result={"flushed": True},
            )
        )

    @staticmethod
    def _extract_transcript_text(data) -> str:
        def _get_text(obj):
            if not isinstance(obj, dict):
                return ""
            for key in ("transcript", "text", "normalized_text"):
                val = obj.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip()
            words = obj.get("words")
            if isinstance(words, list):
                joined = " ".join(
                    w.get("text", "").strip() for w in words if isinstance(w, dict)
                ).strip()
                if joined:
                    return joined
            return ""

        primary = _get_text(data)
        if primary:
            return primary
        nested = data.get("data") if isinstance(data, dict) else None
        if isinstance(nested, dict):
            return _get_text(nested)
        return ""

    @staticmethod
    def _parse_text_replacements(raw: str) -> List[Tuple[re.Pattern, str]]:
        pairs: List[Tuple[str, str]] = []
        if not raw:
            return []
        for item in raw.split("|"):
            item = item.strip()
            if not item or "=>" not in item:
                continue
            src, dst = item.split("=>", 1)
            src = src.strip()
            dst = dst.strip()
            if not src or not dst:
                continue
            pairs.append((src, dst))
        # Prefer longer matches first to avoid partial overrides.
        pairs.sort(key=lambda x: len(x[0]), reverse=True)
        compiled: List[Tuple[re.Pattern, str]] = []
        for src, dst in pairs:
            pattern = re.compile(rf"\b{re.escape(src)}\b", re.IGNORECASE)
            compiled.append((pattern, dst))
        return compiled

    def _apply_text_replacements(self, text: str) -> str:
        if not text:
            return ""
        updated = text
        for pattern, replacement in self._text_replacements:
            updated = pattern.sub(replacement, updated)
        return " ".join(updated.split()).strip()

    @staticmethod
    def _normalize_language(language: str) -> str:
        if not language:
            return "en"
        cleaned = language.strip().lower()
        if "-" in cleaned:
            cleaned = cleaned.split("-", 1)[0]
        return cleaned

    def _language_enum(self) -> Optional[Language]:
        try:
            return Language(self._language)
        except Exception:
            return None

    @staticmethod
    def _resolve_commit_strategy(value: str) -> CommitStrategy:
        if (value or "").lower() == "manual":
            return CommitStrategy.MANUAL
        return CommitStrategy.VAD

    @staticmethod
    def _resolve_audio_format(value: str) -> Optional[AudioFormat]:
        candidate = (value or "").strip().lower()
        for fmt in AudioFormat:
            if fmt.value == candidate:
                return fmt
        return None

    @staticmethod
    def _format_for_rate(sample_rate: int) -> AudioFormat:
        if sample_rate == 8000:
            return AudioFormat.PCM_8000
        if sample_rate == 16000:
            return AudioFormat.PCM_16000
        if sample_rate == 22050:
            return AudioFormat.PCM_22050
        if sample_rate == 24000:
            return AudioFormat.PCM_24000
        if sample_rate == 44100:
            return AudioFormat.PCM_44100
        if sample_rate == 48000:
            return AudioFormat.PCM_48000
        return AudioFormat.PCM_16000
