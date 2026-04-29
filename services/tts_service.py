import asyncio
import base64
import os
from abc import ABC, abstractmethod
from typing import Any, Dict

import aiohttp
from dotenv import load_dotenv

from logger_config import get_logger
from services.event_emmiter import EventEmitter
from services.redis_client import get_redis_client

load_dotenv()
logger = get_logger("TTS")

redis_client = get_redis_client()


class AbstractTTSService(EventEmitter, ABC):
    def __init__(self):
        super().__init__()
        self.sample_rate = 8000
        self.frame_ms = int(os.getenv("TTS_FRAME_MS", "20"))
        self.frame_bytes = max(1, int(self.sample_rate * self.frame_ms / 1000))
        self._playback_index = 0
        self._index_lock = asyncio.Lock()

    def reset(self):
        self._playback_index = 0

    async def _next_audio_index(self) -> int:
        async with self._index_lock:
            current = self._playback_index
            self._playback_index += 1
            return current

    async def _emit_frame(self, frame_bytes: bytes, label: str, interaction_count: int):
        if not frame_bytes:
            return
        frame_len = len(frame_bytes)
        if frame_len != self.frame_bytes:
            logger.warning(
                "TTS frame size mismatch (expected %d bytes, got %d).",
                self.frame_bytes,
                frame_len
            )
        payload = base64.b64encode(frame_bytes).decode('utf-8')
        frame_index = await self._next_audio_index()
        await self.emit('speech', frame_index, payload, label, interaction_count)

    async def _drain_frame_buffer(
        self,
        buffer: bytearray,
        label: str,
        interaction_count: int,
        pad_tail: bool = False
    ):
        while len(buffer) >= self.frame_bytes:
            frame = bytes(buffer[:self.frame_bytes])
            del buffer[:self.frame_bytes]
            await self._emit_frame(frame, label, interaction_count)

        if pad_tail and buffer:
            padding = bytes([0x7F]) * (self.frame_bytes - len(buffer))
            frame = bytes(buffer) + padding
            buffer.clear()
            await self._emit_frame(frame, label, interaction_count)

    async def _stream_audio_bytes(self, audio_bytes: bytes, label: str, interaction_count: int):
        frame_buffer = bytearray(audio_bytes)
        await self._drain_frame_buffer(frame_buffer, label, interaction_count, pad_tail=True)

    @abstractmethod
    async def generate(self, llm_reply: Dict[str, Any], interaction_count: int):
        pass

    @abstractmethod
    async def set_voice(self, voice_id: str):
        pass

    @abstractmethod
    async def disconnect(self):
        pass


class ElevenLabsTTS(AbstractTTSService):
    def __init__(self, voice_id):
        super().__init__()
        if not voice_id:
            raise ValueError("Voice ID must be provided for ElevenLabsTTS")
        self.voice_id = voice_id
        self.api_key = os.getenv("ELEVENLABS_API_KEY")
        self.model_id = os.getenv("ELEVENLABS_MODEL_ID")
        self._timeout = aiohttp.ClientTimeout(total=float(os.getenv("ELEVENLABS_TIMEOUT", "18")))
        # Smaller chunks reduce time-to-first-frame in interactive mode.
        default_chunk = max(self.frame_bytes, 160)
        self._stream_chunk_size = int(os.getenv("ELEVENLABS_STREAM_CHUNK_SIZE", str(default_chunk)))
        # Reuse a single HTTP session per call to avoid paying TLS/DNS overhead on every TTS request.
        self._session: aiohttp.ClientSession | None = None
        self._session_lock = asyncio.Lock()
        self._connector: aiohttp.BaseConnector | None = None
        latency_setting = int(os.getenv("ELEVENLABS_STREAM_LATENCY", "4"))
        self._optimize_latency = max(0, min(latency_setting, 4))
        logger.info(f"[ElevenLabsTTS] Using Voice ID: {self.voice_id}")

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session and not self._session.closed:
                return self._session
            # Keepalive + DNS caching help a lot for interactive TTS.
            keepalive = float(os.getenv("ELEVENLABS_KEEPALIVE_TIMEOUT_SEC", "30"))
            ttl_dns = int(os.getenv("ELEVENLABS_DNS_CACHE_TTL_SEC", "300"))
            self._connector = aiohttp.TCPConnector(
                limit=0,
                ttl_dns_cache=max(0, ttl_dns),
                keepalive_timeout=max(0.0, keepalive),
            )
            self._session = aiohttp.ClientSession(timeout=self._timeout, connector=self._connector)
            return self._session

    def set_optimize_latency(self, value: int) -> None:
        # ElevenLabs supports optimize_streaming_latency in [0..4].
        try:
            self._optimize_latency = max(0, min(int(value), 4))
        except Exception:
            return

    def set_voice(self, voice_id):
        self.voice_id = voice_id
        logger.info(f"[ElevenLabsTTS] Voice ID updated to: {self.voice_id}")

    def reset(self):
        super().reset()

    async def disconnect(self):
        # Close per-call HTTP session (keeps connections clean under load).
        async with self._session_lock:
            if self._session and not self._session.closed:
                try:
                    await self._session.close()
                except Exception:
                    pass
            self._session = None
            self._connector = None

    async def _stream_cached_audio(self, cached_audio: bytes, partial_response: str, interaction_count: int):
        await self._stream_audio_bytes(cached_audio, partial_response, interaction_count)

    async def generate(self, llm_reply: Dict[str, Any], interaction_count: int):
        logger.info(f"[ElevenLabsTTS] Generating TTS with Voice ID: {self.voice_id}")
        partial_response = llm_reply['partialResponse']

        if not partial_response:
            logger.error("Partial response is empty. Skipping TTS generation.")
            return

        cache_enabled = os.getenv("TTS_CACHE_ENABLED", "true").lower() == "true"
        cache_bust = os.getenv("TTS_CACHE_BUST", "").strip()
        cache_key = f"tts:{self.voice_id}:{cache_bust}:{partial_response}:elevenlabs"
        cached_audio = redis_client.get(cache_key) if cache_enabled else None

        if cached_audio:
            logger.info(f"Using cached TTS response for: {partial_response}")
            await self._stream_cached_audio(base64.b64decode(cached_audio), partial_response, interaction_count)
            return

        frame_buffer = bytearray()
        cache_buffer = bytearray()

        try:
            output_format = os.getenv("ELEVENLABS_TTS_OUTPUT_FORMAT", "ulaw_8000")
            url = f"https://api.elevenlabs.io/v1/text-to-speech/{self.voice_id}/stream"
            headers = {
                "xi-api-key": self.api_key,
                "Content-Type": "application/json",
            }
            params = {
                "output_format": output_format,
                "optimize_streaming_latency": self._optimize_latency
            }
            data = {"model_id": self.model_id, "text": partial_response}

            session = await self._get_session()
            started = asyncio.get_running_loop().time()
            first_byte_ts: float | None = None
            async with session.post(url, headers=headers, params=params, json=data) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch TTS audio. HTTP status: {response.status}")
                    logger.error(f"Response: {await response.text()}")
                    return

                async for chunk in response.content.iter_chunked(self._stream_chunk_size):
                    if not chunk:
                        continue
                    if first_byte_ts is None:
                        first_byte_ts = asyncio.get_running_loop().time()
                        logger.info(
                            "[ElevenLabsTTS] first_bytes in {}ms (optimize={})",
                            int((first_byte_ts - started) * 1000.0),
                            self._optimize_latency,
                        )
                    cache_buffer.extend(chunk)
                    frame_buffer.extend(chunk)
                    await self._drain_frame_buffer(frame_buffer, partial_response, interaction_count)

            await self._drain_frame_buffer(frame_buffer, partial_response, interaction_count, pad_tail=True)

            if cache_enabled and cache_buffer:
                ttl_sec = int(os.getenv("TTS_CACHE_TTL_SEC", "3600"))
                audio_base64 = base64.b64encode(bytes(cache_buffer)).decode('utf-8')
                redis_client.set(cache_key, audio_base64, ex=max(ttl_sec, 1))
        except asyncio.CancelledError:
            logger.info("ElevenLabs TTS streaming cancelled.")
            raise
        except Exception:
            logger.error("Error occurred in ElevenLabs TTS service", exc_info=True)


class TTSFactory:
    @staticmethod
    def get_tts_service(service_name: str, voice_id: str) -> AbstractTTSService:
        if not voice_id:
            raise ValueError("Voice ID must be provided")

        if service_name.lower() == "elevenlabs":
            return ElevenLabsTTS(voice_id=voice_id)
        else:
            raise ValueError(f"Unsupported TTS service: {service_name}")
