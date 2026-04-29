import asyncio
import audioop
import base64
import json
import os
import time
from collections import deque
from typing import Deque, Optional, Tuple, Union

import numpy as np

import requests
import websockets
from websockets.exceptions import ConnectionClosed

try:
    import webrtcvad  # type: ignore
except ImportError:
    webrtcvad = None

try:
    from websockets.asyncio.client import connect as asyncio_ws_connect  # type: ignore
except ImportError:
    asyncio_ws_connect = None
else:
    current_connect = getattr(websockets, "connect", None)
    if current_connect is None or "legacy" in getattr(current_connect, "__module__", ""):
        websockets.connect = asyncio_ws_connect

from elevenlabs.realtime.connection import RealtimeConnection, RealtimeEvents
from elevenlabs.realtime.scribe import (
    AudioFormat,
    CommitStrategy,
    RealtimeAudioOptions,
    ScribeRealtime,
)

from logger_config import get_logger
from services.event_emmiter import EventEmitter
from services.noise_suppression import NoiseSuppressor

logger = get_logger("Transcription")


class TranscriptionService(EventEmitter):
    """
    Streams Twilio audio into ElevenLabs' Scribe v2 realtime endpoint.

    Twilio streams 8 kHz mu-law audio frames; Scribe requires 16 kHz PCM.
    Each payload is decoded, upsampled, and forwarded over ElevenLabs'
    WebSocket connection so we can receive partial and final transcripts
    with <500 ms latency.
    """

    def __init__(self):
        super().__init__()
        api_key = os.getenv("ELEVENLABS_API_KEY")
        if not api_key:
            raise ValueError("ELEVENLABS_API_KEY is missing")

        self._scribe = ScribeRealtime(api_key=api_key)
        self._connection: Optional[RealtimeConnection] = None
        self._connect_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()

        self.stream_sid: Optional[str] = None
        self.is_connected = False
        self.current_language = "en"
        self._max_reconnect_attempts = int(
            os.getenv("ELEVENLABS_STT_MAX_RECONNECTS", "3")
        )
        self._reconnect_delay = float(
            os.getenv("ELEVENLABS_STT_RECONNECT_DELAY_SEC", "0.5")
        )
        self._last_connect_time = 0.0
        self._session_id: Optional[str] = None

        self.model_id = os.getenv("ELEVENLABS_STT_MODEL_ID", "scribe_v2_realtime")
        self.sample_rate = int(os.getenv("ELEVENLABS_STT_SAMPLE_RATE", "16000"))
        self.audio_format = self._resolve_audio_format(
            os.getenv("ELEVENLABS_STT_AUDIO_FORMAT", "pcm_16000")
        )
        self.commit_strategy = self._resolve_commit_strategy(
            os.getenv("ELEVENLABS_STT_COMMIT_STRATEGY", "vad")
        )
        self.vad_silence_threshold = self._get_float(
            "ELEVENLABS_STT_VAD_SILENCE_THRESHOLD", default=0.6
        )
        self.vad_threshold = self._get_float(
            "ELEVENLABS_STT_VAD_THRESHOLD", default=0.4
        )
        self.min_speech_ms = self._get_int("ELEVENLABS_STT_MIN_SPEECH_MS", default=60)
        self.min_silence_ms = self._get_int(
            "ELEVENLABS_STT_MIN_SILENCE_MS", default=80
        )
        self.flush_delay = float(os.getenv("TRANSCRIPT_FLUSH_SEC", "0.15"))
        self._flush_task: Optional[asyncio.Task] = None
        self._pending_partial: str = ""
        self._manual_commit_lock = asyncio.Lock()
        self._pcm_buffer = bytearray()
        self._chunk_ms = int(os.getenv("TWILIO_STT_CHUNK_MS", "60"))
        self._chunk_bytes = max(
            2, int(self.sample_rate * self._chunk_ms / 1000) * 2
        )
        self._speech_rms_threshold = int(
            os.getenv("USER_SPEECH_RMS_THRESHOLD", "220")
        )
        self._speech_start_debounce = float(
            os.getenv("USER_SPEECH_START_DEBOUNCE_SEC", "0.12")
        )
        self._last_speech_start_ts = 0.0
        self._noise_suppression_enabled = (
            os.getenv("ENABLE_NOISE_SUPPRESSION", "true").lower() == "true"
        )
        self._vad = None
        self._vad_frame_ms = self._get_int("NOISE_VAD_FRAME_MS", 20)
        if self._vad_frame_ms not in (10, 20, 30):
            logger.warning(
                "NOISE_VAD_FRAME_MS must be 10, 20, or 30; using 20 ms instead."
            )
            self._vad_frame_ms = 20
        self._noise_hangover_frames = max(
            0, self._get_int("NOISE_HANGOVER_FRAMES", 4)
        )
        self._noise_pre_buffer_frames = max(
            0, self._get_int("NOISE_PRE_BUFFER_FRAMES", 3)
        )
        self._vad_frame_bytes = max(
            2, int(self.sample_rate * self._vad_frame_ms / 1000) * 2
        )
        self._vad_buffer = bytearray()
        self._pre_speech_frames = deque(maxlen=self._noise_pre_buffer_frames or 1)
        self._vad_hangover = 0
        self._transient_suppress_ms = max(self._get_int("TRANSIENT_SUPPRESS_MS", 60), 20)
        self._speech_release_frames = max(
            1, int(round(self._transient_suppress_ms / self._vad_frame_ms))
        )
        self._analysis_eps = 1e-6
        self._speech_confidence_min = float(os.getenv("SPEECH_CONFIDENCE_MIN", "0.25"))
        self._speech_snr_min = float(os.getenv("SPEECH_SNR_MIN_DB", "0.5"))
        self._speech_snr_target = float(os.getenv("SPEECH_SNR_TARGET_DB", "10.0"))
        self._speech_voice_min = float(os.getenv("SPEECH_VOICE_RATIO_MIN", "0.15"))
        self._speech_voice_max = float(os.getenv("SPEECH_VOICE_RATIO_MAX", "0.9"))
        self._speech_crest_max = float(os.getenv("SPEECH_CREST_MAX", "18.0"))
        self._speech_noise_baseline = float(os.getenv("SPEECH_NOISE_BASELINE", "600.0"))
        self._speech_noise_floor = self._speech_noise_baseline
        self._speech_noise_decay = float(os.getenv("SPEECH_NOISE_DECAY", "0.97"))
        self._voice_band_low = float(os.getenv("SPEECH_VOICE_BAND_LOW_HZ", "200.0"))
        self._voice_band_high = float(os.getenv("SPEECH_VOICE_BAND_HIGH_HZ", "3600.0"))
        if self._voice_band_high <= self._voice_band_low:
            self._voice_band_high = self._voice_band_low + 200.0
        self._analysis_frame_samples = max(2, self._vad_frame_bytes // 2)
        self._analysis_window = np.hanning(self._analysis_frame_samples).astype(np.float32)
        self._analysis_fft_len = max(1, 1 << (self._analysis_frame_samples - 1).bit_length())
        freqs = np.fft.rfftfreq(self._analysis_fft_len, d=1.0 / self.sample_rate)
        mask = np.logical_and(
            freqs >= self._voice_band_low,
            freqs <= self._voice_band_high,
        )
        if not np.any(mask):
            mask = np.ones_like(freqs, dtype=bool)
        self._voice_band_mask = mask
        self._pending_speech_frames: Deque[bytes] = deque()
        self._speech_frames_run = 0
        self._speech_release_active = False
        self._suppressor_last_output_speech = False
        self._gate_passthrough = (
            os.getenv("NOISE_GATE_PASSTHROUGH_NO_VAD", "true").lower() == "true"
        )
        self._noise_suppressor: Optional[NoiseSuppressor] = None
        if self._noise_suppression_enabled:
            if webrtcvad is None:
                logger.warning(
                    "ENABLE_NOISE_SUPPRESSION is true but the 'webrtcvad' package "
                    "is not installed. Falling back to spectral gating only."
                )
                if self._gate_passthrough:
                    logger.info(
                        "Noise gate passthrough enabled (no webrtcvad); forwarding low-confidence audio."
                    )
            else:
                aggressiveness = min(
                    3, max(0, self._get_int("NOISE_VAD_AGGRESSIVENESS", 2))
                )
                self._vad = webrtcvad.Vad(aggressiveness)
            try:
                frame_ms = self._get_int("NOISE_SUPPRESSOR_FRAME_MS", 20)
                hop_ms = self._get_int("NOISE_SUPPRESSOR_HOP_MS", 10)
                strength = float(os.getenv("NOISE_SUPPRESSOR_STRENGTH", "0.65"))
                speech_floor = float(
                    os.getenv("NOISE_SUPPRESSOR_SPEECH_GAIN_FLOOR", "0.25")
                )
                silence_floor = float(
                    os.getenv("NOISE_SUPPRESSOR_SILENCE_GAIN_FLOOR", "0.05")
                )
                smoothing = float(os.getenv("NOISE_PROFILE_SMOOTHING", "0.92"))
                click_level = int(os.getenv("NOISE_CLICK_GUARD_LEVEL", "12000"))
                self._noise_suppressor = NoiseSuppressor(
                    sample_rate=self.sample_rate,
                    frame_ms=frame_ms,
                    hop_ms=hop_ms,
                    suppression_strength=strength,
                    speech_gain_floor=speech_floor,
                    silence_gain_floor=silence_floor,
                    noise_smoothing=smoothing,
                    click_guard_level=click_level,
                    vad=self._vad,
                )
                logger.info(
                    "Noise suppression enabled (frame=%dms, hop=%dms, strength=%.2f).",
                    frame_ms,
                    hop_ms,
                    strength,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to initialize advanced noise suppressor: %s. "
                    "Falling back to spectral-only gating.",
                    exc,
                )
                self._noise_suppressor = None

    def set_stream_sid(self, stream_id: str):
        self.stream_sid = stream_id
        self._pcm_buffer.clear()
        self._last_speech_start_ts = 0.0
        self._reset_noise_buffers()
        if self._noise_suppressor:
            self._noise_suppressor.reset()

    def get_stream_sid(self) -> Optional[str]:
        return self.stream_sid

    async def connect(self, language: Optional[str] = None):
        """Establish a realtime ElevenLabs connection."""
        async with self._connect_lock:
            if self._connection:
                logger.warning("Already connected to ElevenLabs STT.")
                return

            options: RealtimeAudioOptions = {
                "model_id": self.model_id,
                "audio_format": self.audio_format,
                "sample_rate": self.sample_rate,
                "commit_strategy": self.commit_strategy,
            }

            if self.commit_strategy == CommitStrategy.VAD:
                options["vad_silence_threshold_secs"] = self.vad_silence_threshold
                options["vad_threshold"] = self.vad_threshold
                options["min_speech_duration_ms"] = self.min_speech_ms
                options["min_silence_duration_ms"] = self.min_silence_ms

            if language:
                self.current_language = language
            if self.current_language:
                # Scribe expects ISO-639 codes; map en-US -> en.
                options["language_code"] = self.current_language.split("-")[0].lower()

            start = time.monotonic()
            logger.info(
                f"Connecting to ElevenLabs STT ({self.model_id}, "
                f"{self.audio_format.value}, {self.sample_rate} Hz, strategy={self.commit_strategy.value}, "
                f"language={self.current_language})"
            )

            connection = await self._scribe.connect(options)
            connection.on(RealtimeEvents.PARTIAL_TRANSCRIPT, self._handle_partial)
            connection.on(
                RealtimeEvents.COMMITTED_TRANSCRIPT,
                self._handle_final_transcript,
            )
            connection.on(
                RealtimeEvents.COMMITTED_TRANSCRIPT_WITH_TIMESTAMPS,
                self._handle_final_transcript,
            )
            connection.on(RealtimeEvents.ERROR, self._handle_error)
            connection.on(RealtimeEvents.CLOSE, self._handle_close)
            connection.on(
                RealtimeEvents.SESSION_STARTED,
                self._handle_session_started,
            )

            self._connection = connection
            self.is_connected = True
            self._last_connect_time = time.monotonic()
            duration = self._last_connect_time - start
            logger.info(
                f"Connected to ElevenLabs STT (session={self._session_id}) "
                f"in {duration:.2f}s"
            )

    async def ensure_connected(self, language: Optional[str] = None):
        """Ensure there is an active STT session before sending audio."""
        if language:
            self.current_language = language
        if self.is_connected and self._connection:
            return

        attempt = 0
        while attempt < self._max_reconnect_attempts:
            attempt += 1
            try:
                await self.connect(self.current_language)
                return
            except Exception as exc:
                logger.warning(
                    "ElevenLabs STT connect attempt {attempt}/{max_attempts} failed: {error}",
                    attempt=attempt,
                    max_attempts=self._max_reconnect_attempts,
                    error=exc,
                )
                if attempt < self._max_reconnect_attempts:
                    await asyncio.sleep(self._reconnect_delay * attempt)

        raise RuntimeError(
            "Unable to connect to ElevenLabs STT after "
            f"{self._max_reconnect_attempts} attempts."
        )

    async def send(self, payload: Union[bytes, bytearray, str]):
        """Convert mu-law 8 kHz audio to PCM16 16 kHz and stream it."""
        if not payload:
            return
        if isinstance(payload, str):
            try:
                payload = base64.b64decode(payload)
            except Exception as exc:
                logger.error("Failed to decode base64 audio payload: %s", exc)
                return
        elif isinstance(payload, bytearray):
            payload = bytes(payload)
        try:
            await self.ensure_connected()
        except RuntimeError as exc:
            logger.error("Aborting audio send: %s", exc)
            return

        if not self._connection:
            logger.error("Cannot send audio; ElevenLabs STT connection missing.")
            return

        converted = self._convert_twilio_payload(payload)
        if not converted:
            return
        pcm, rms = converted

        speech_gate_ok = True
        if self._noise_suppression_enabled:
            pcm = self._apply_noise_suppression(pcm)
            if not pcm:
                return
            if len(pcm) >= 2:
                rms = audioop.rms(pcm, 2)
            speech_gate_ok = self._suppressor_last_output_speech

        if rms >= self._speech_rms_threshold and speech_gate_ok:
            await self._maybe_emit_speech_start()

        self._pcm_buffer.extend(pcm)
        await self._flush_audio_chunks()

    async def _flush_audio_chunks(self, force: bool = False):
        if not self._connection:
            return
        while len(self._pcm_buffer) >= self._chunk_bytes:
            chunk = bytes(self._pcm_buffer[:self._chunk_bytes])
            del self._pcm_buffer[:self._chunk_bytes]
            await self._send_pcm_chunk(chunk)

        if force and self._pcm_buffer:
            chunk = bytes(self._pcm_buffer)
            self._pcm_buffer.clear()
            await self._send_pcm_chunk(chunk)

    async def _send_pcm_chunk(self, chunk: bytes):
        if not chunk:
            return
        chunk_b64 = base64.b64encode(chunk).decode("utf-8")
        async with self._send_lock:
            for attempt in range(2):
                try:
                    logger.debug(
                        "Streaming audio chunk to ElevenLabs (pcm=%d bytes, streamSid=%s)",
                        len(chunk),
                        self.stream_sid,
                    )
                    await self._connection.send({
                        "audio_base_64": chunk_b64,
                        "sample_rate": self.sample_rate,
                    })
                    break
                except (ConnectionClosed, RuntimeError) as exc:
                    self.is_connected = False
                    logger.warning(
                        "ElevenLabs STT send failed (attempt %d); reconnecting... %s",
                        attempt + 1,
                        exc,
                    )
                    if attempt == 0:
                        try:
                            await self.ensure_connected()
                        except RuntimeError as reconnect_exc:
                            logger.error(
                                "Reconnect during send failed: %s", reconnect_exc
                            )
                            break
                        continue
                    logger.error("Unable to stream audio after reconnect: %s", exc)
                except Exception as exc:
                    logger.error(f"Error streaming audio to ElevenLabs: {exc}")
                    break

    async def disconnect(self):
        async with self._connect_lock:
            if not self._connection:
                return
            logger.info("Disconnecting ElevenLabs STT session.")
            try:
                if self._noise_suppressor:
                    tail = self._noise_suppressor.flush()
                    if tail:
                        self._pcm_buffer.extend(tail)
                await self._flush_audio_chunks(force=True)
                await self._commit_manual()
                await self._connection.close()
            finally:
                self._connection = None
                self.is_connected = False
                self._reset_noise_buffers()

    # ElevenLabs event handlers -------------------------------------------------
    def _handle_partial(self, data):
        message_type = (data.get("message_type") or "").lower()
        if message_type == "input_audio_buffer_speech_started":
            logger.info("ElevenLabs detected speech start (streamSid=%s)", self.stream_sid)
            asyncio.create_task(self.emit("speech_start", self.stream_sid))
            return
        transcript = self._extract_transcript_text(data)
        if not transcript:
            return
        logger.debug(
            "ElevenLabs partial transcript (streamSid=%s): %s",
            self.stream_sid,
            transcript,
        )
        self._pending_partial = transcript
        self._schedule_forced_flush()
        asyncio.create_task(self.emit("utterance", transcript, self.stream_sid))
        asyncio.create_task(self.emit("partial_transcription", transcript))

    def _handle_final_transcript(self, data):
        transcript = self._extract_transcript_text(data)
        if not transcript:
            return
        logger.info(
            "ElevenLabs committed transcript (streamSid=%s): %s",
            self.stream_sid,
            transcript,
        )
        self._pending_partial = ""
        self._cancel_flush_task()
        asyncio.create_task(self.emit("transcription", transcript))

    def _handle_error(self, data):
        logger.error(f"ElevenLabs STT error: {data}")
        self.is_connected = False
        if self._connection:
            asyncio.create_task(self._connection.close())

    def _handle_close(self, *_):
        logger.info("ElevenLabs STT connection closed.")
        self._connection = None
        self.is_connected = False
        self._pending_partial = ""
        self._cancel_flush_task()
        self._session_id = None
        self._last_speech_start_ts = 0.0

    def _handle_session_started(self, data):
        session_id = data.get("session_id")
        if session_id:
            self._session_id = session_id
            logger.info(f"ElevenLabs STT session ready (id={session_id}).")

    # Helpers -------------------------------------------------------------------
    def _convert_twilio_payload(self, payload: bytes) -> Optional[Tuple[bytes, float]]:
        """Convert 8 kHz mu-law audio to PCM16 at the configured sample rate and return RMS."""
        try:
            pcm_8k = audioop.ulaw2lin(payload, 2)
            rms = audioop.rms(pcm_8k, 2)
            if self.sample_rate == 8000:
                return pcm_8k, rms
            converted, _ = audioop.ratecv(pcm_8k, 2, 1, 8000, self.sample_rate, None)
            return converted, rms
        except Exception as exc:
            logger.error(f"Failed to convert audio payload: {exc}")
            return None

    def _reset_noise_buffers(self):
        self._vad_buffer = bytearray()
        self._pre_speech_frames.clear()
        self._vad_hangover = 0
        if self._noise_suppressor:
            self._noise_suppressor.reset()
        self._pending_speech_frames.clear()
        self._speech_frames_run = 0
        self._speech_release_active = False
        self._suppressor_last_output_speech = False
        self._speech_noise_floor = self._speech_noise_baseline

    def _ensure_analysis_window(self, sample_count: int) -> None:
        if sample_count == self._analysis_frame_samples:
            return
        samples = max(2, sample_count)
        self._analysis_frame_samples = samples
        self._analysis_window = np.hanning(samples).astype(np.float32)
        self._analysis_fft_len = max(1, 1 << (samples - 1).bit_length())
        freqs = np.fft.rfftfreq(self._analysis_fft_len, d=1.0 / self.sample_rate)
        mask = np.logical_and(freqs >= self._voice_band_low, freqs <= self._voice_band_high)
        if not np.any(mask):
            mask = np.ones_like(freqs, dtype=bool)
        self._voice_band_mask = mask

    def _frame_is_confident_speech(self, frame: bytes, vad_flag: bool) -> bool:
        if not frame:
            return vad_flag
        samples = np.frombuffer(frame, dtype=np.int16)
        if samples.size == 0:
            return vad_flag
        if samples.size != self._analysis_frame_samples:
            self._ensure_analysis_window(samples.size)
        floats = samples.astype(np.float32)
        rms = float(np.sqrt(np.mean(np.square(floats)))) + self._analysis_eps
        peak = float(np.max(np.abs(floats))) + self._analysis_eps
        crest = peak / rms if rms else float("inf")
        windowed = floats * self._analysis_window
        spectrum = np.fft.rfft(windowed, n=self._analysis_fft_len)
        magnitudes = np.abs(spectrum)
        total_energy = float(np.sum(magnitudes)) + self._analysis_eps
        voice_energy = float(np.sum(magnitudes[self._voice_band_mask]))
        voice_ratio = voice_energy / total_energy if total_energy else 0.0
        self._speech_noise_floor = (
            self._speech_noise_decay * self._speech_noise_floor
            + (1.0 - self._speech_noise_decay) * rms
        )
        snr = 0.0
        if self._speech_noise_floor > 0:
            snr = 20.0 * np.log10(
                (rms + self._analysis_eps) / (self._speech_noise_floor + self._analysis_eps)
            )
        score = 0.0
        if vad_flag:
            score += 0.45
        if snr >= self._speech_snr_min:
            span = max(self._analysis_eps, self._speech_snr_target - self._speech_snr_min)
            score += min(0.35, (snr - self._speech_snr_min) / span * 0.35)
        if self._speech_voice_min <= voice_ratio <= self._speech_voice_max:
            score += 0.2
        if crest <= self._speech_crest_max:
            score += 0.1
        return score >= self._speech_confidence_min

    def _apply_noise_suppression(self, pcm: bytes) -> bytes:
        if not pcm:
            self._suppressor_last_output_speech = False
            return pcm

        if not self._noise_suppression_enabled:
            self._suppressor_last_output_speech = bool(pcm)
            return pcm

        processed = pcm
        if self._noise_suppressor:
            try:
                processed = self._noise_suppressor.process(pcm)
            except Exception as exc:
                logger.warning("Noise suppressor runtime failure: %s", exc)
                self._noise_suppressor = None
                processed = pcm

        if not processed:
            if self._gate_passthrough:
                try:
                    rms = audioop.rms(pcm, 2)
                except Exception:
                    rms = 0
                self._suppressor_last_output_speech = rms >= self._speech_rms_threshold
                return pcm
            self._suppressor_last_output_speech = False
            return b""

        if self._gate_passthrough:
            try:
                rms = audioop.rms(processed, 2)
            except Exception:
                rms = 0
            self._suppressor_last_output_speech = rms >= self._speech_rms_threshold
            return processed

        self._vad_buffer.extend(processed)
        frame_bytes = self._vad_frame_bytes
        output = bytearray()
        speech_output = False
        while len(self._vad_buffer) >= frame_bytes:
            frame = bytes(self._vad_buffer[:frame_bytes])
            del self._vad_buffer[:frame_bytes]
            if self._vad:
                try:
                    speech_detected = self._vad.is_speech(frame, self.sample_rate)
                except Exception as exc:
                    logger.warning(f"Noise suppression VAD failure: {exc}")
                    speech_detected = True
            else:
                speech_detected = audioop.rms(frame, 2) >= self._speech_rms_threshold

            confident_speech = self._frame_is_confident_speech(frame, speech_detected)

            if confident_speech:
                if self._speech_release_active:
                    if self._pre_speech_frames:
                        for buffered in self._pre_speech_frames:
                            output.extend(buffered)
                        self._pre_speech_frames.clear()
                    output.extend(frame)
                    speech_output = True
                else:
                    self._speech_frames_run += 1
                    self._pending_speech_frames.append(frame)
                    if self._speech_frames_run >= self._speech_release_frames:
                        self._speech_release_active = True
                        if self._pre_speech_frames:
                            for buffered in self._pre_speech_frames:
                                output.extend(buffered)
                            self._pre_speech_frames.clear()
                        while self._pending_speech_frames:
                            output.extend(self._pending_speech_frames.popleft())
                        speech_output = True
                self._vad_hangover = self._noise_hangover_frames
            else:
                self._speech_frames_run = 0
                if self._speech_release_active:
                    if self._vad_hangover > 0:
                        output.extend(frame)
                        speech_output = True
                        self._vad_hangover -= 1
                    else:
                        self._speech_release_active = False
                        self._pending_speech_frames.clear()
                        self._pre_speech_frames.clear()
                else:
                    self._pending_speech_frames.clear()
                    if self._noise_pre_buffer_frames > 0:
                        self._pre_speech_frames.append(frame)
        self._suppressor_last_output_speech = speech_output
        return bytes(output)

    async def _maybe_emit_speech_start(self):
        now = time.monotonic()
        if (now - self._last_speech_start_ts) < self._speech_start_debounce:
            return
        if not self.stream_sid:
            return
        self._last_speech_start_ts = now
        await self.emit("speech_start", self.stream_sid)

    def _resolve_audio_format(self, raw: str) -> AudioFormat:
        try:
            return AudioFormat(raw)
        except ValueError:
            logger.warning(f"Unsupported ELEVENLABS_STT_AUDIO_FORMAT '{raw}', using pcm_16000.")
            return AudioFormat.PCM_16000

    def _resolve_commit_strategy(self, raw: str) -> CommitStrategy:
        try:
            return CommitStrategy(raw.lower())
        except ValueError:
            logger.warning(f"Unsupported ELEVENLABS_STT_COMMIT_STRATEGY '{raw}', using VAD.")
            return CommitStrategy.VAD

    def _get_float(self, key: str, default: float) -> float:
        value = os.getenv(key)
        if value is None:
            return default
        try:
            return float(value)
        except ValueError:
            logger.warning(f"Invalid float for {key}: {value}. Using default {default}.")
            return default

    def _get_int(self, key: str, default: int) -> int:
        value = os.getenv(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            logger.warning(f"Invalid integer for {key}: {value}. Using default {default}.")
            return default

    def _cancel_flush_task(self):
        task = self._flush_task
        if task:
            task.cancel()
        self._flush_task = None

    def _schedule_forced_flush(self):
        if self.flush_delay <= 0 or not self._pending_partial:
            return
        self._cancel_flush_task()
        self._flush_task = asyncio.create_task(self._forced_flush())

    async def _forced_flush(self):
        task = asyncio.current_task()
        try:
            await asyncio.sleep(self.flush_delay)
            if not self._pending_partial:
                return
            logger.info(
                f"Forcing ElevenLabs transcript flush after {self.flush_delay}s of silence."
            )
            pending = self._pending_partial
            self._pending_partial = ""
            await self.emit("transcription", pending)
            await self._commit_manual()
        except asyncio.CancelledError:
            pass
        finally:
            if self._flush_task is task:
                self._flush_task = None

    async def _commit_manual(self):
        """Send a manual commit when the strategy requires it."""
        if self.commit_strategy != CommitStrategy.MANUAL:
            return
        if not self._connection:
            return
        async with self._manual_commit_lock:
            try:
                await self._connection.commit()
                logger.debug("Sent manual ElevenLabs commit to finalize transcript.")
            except Exception as exc:
                logger.warning(f"Failed to commit ElevenLabs transcript segment: {exc}")

    @staticmethod
    def _extract_transcript_text(data) -> str:
        """
        ElevenLabs currently returns partial transcripts under the `text` key while committed
        payloads may use either `transcript` or `text`. Normalize by checking both so we stay
        compatible with future API changes.
        """
        for key in ("transcript", "text"):
            text = (data.get(key) or "").strip()
            if text:
                return text
        return ""
