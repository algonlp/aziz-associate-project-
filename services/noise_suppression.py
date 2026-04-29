"""Streaming spectral noise suppression tailored for Twilio → ElevenLabs audio."""
from __future__ import annotations

import math
import os
from typing import Optional

import numpy as np

try:  # pragma: no cover - optional dependency
    import webrtcvad  # type: ignore
except ImportError:  # pragma: no cover
    webrtcvad = None

from logger_config import get_logger

logger = get_logger("NoiseSuppressor")


class NoiseSuppressor:
    """Applies low-latency spectral gating + click suppression per audio stream."""

    def __init__(
        self,
        sample_rate: int,
        frame_ms: int = 20,
        hop_ms: int = 10,
        suppression_strength: float = 0.65,
        speech_gain_floor: float = 0.25,
        silence_gain_floor: float = 0.05,
        noise_smoothing: float = 0.92,
        click_guard_level: int = 12_000,
        vad: Optional["webrtcvad.Vad"] = None,
    ) -> None:
        if frame_ms <= 0 or hop_ms <= 0:
            raise ValueError("frame_ms and hop_ms must be positive")
        if hop_ms * 2 != frame_ms:
            raise ValueError("frame_ms must be exactly 2× hop_ms for overlap-add to work")

        self.sample_rate = sample_rate
        self.frame_ms = frame_ms
        self.hop_ms = hop_ms
        self._frame_samples = int(sample_rate * frame_ms / 1000)
        self._hop_samples = int(sample_rate * hop_ms / 1000)
        self._frame_bytes = self._frame_samples * 2
        self._hop_bytes = self._hop_samples * 2
        self._window = np.hanning(self._frame_samples).astype(np.float32)
        # Round FFT length up to the next power of two for efficiency.
        self._fft_len = 1 << (self._frame_samples - 1).bit_length()
        self._noise_profile = np.ones(self._fft_len // 2 + 1, dtype=np.float32)
        self._suppression_strength = float(np.clip(suppression_strength, 0.0, 1.5))
        self._speech_gain_floor = float(np.clip(speech_gain_floor, 0.0, 1.0))
        self._silence_gain_floor = float(np.clip(silence_gain_floor, 0.0, 1.0))
        self._noise_smoothing = float(np.clip(noise_smoothing, 0.5, 0.999))
        self._click_guard_level = max(0, int(click_guard_level))
        self._band_count = max(1, int(os.getenv("NOISE_SUPPRESSOR_BANDS", "6")))
        self._band_smoothing = float(os.getenv("NOISE_BAND_SMOOTHING", "0.85"))
        self._band_guard = float(os.getenv("NOISE_BAND_GUARD", "1.15"))
        self._band_floor = float(os.getenv("NOISE_BAND_FLOOR", "0.15"))
        self._band_edges = np.linspace(
            0, self._noise_profile.size, self._band_count + 1, dtype=int
        )
        self._band_noise = np.ones(self._band_count, dtype=np.float32)
        self._transient_crest_threshold = float(os.getenv("TRANSIENT_CREST_THRESHOLD", "8.0"))
        self._transient_rms_ratio = float(os.getenv("TRANSIENT_RMS_RATIO", "1.6"))
        self._max_transient_frames = max(1, int(os.getenv("TRANSIENT_MAX_FRAMES", "2")))
        self._transient_counter = 0
        self._buffer = bytearray()
        self._overlap_tail = np.zeros(self._frame_samples - self._hop_samples, dtype=np.float32)
        self._noise_rms = 600.0
        self._eps = 1e-6
        self._frames_seen = 0
        self._vad = vad
        if self._vad is None and webrtcvad is not None:
            try:
                self._vad = webrtcvad.Vad(2)
            except Exception:  # pragma: no cover - defensive
                self._vad = None

    # --------------------------------------------------------------------- utils
    def reset(self) -> None:
        self._buffer.clear()
        self._overlap_tail.fill(0)
        self._noise_profile.fill(1.0)
        self._band_noise.fill(1.0)
        self._noise_rms = 600.0
        self._frames_seen = 0
        self._transient_counter = 0

    def flush(self) -> bytes:
        if not np.any(self._overlap_tail):
            return b""
        tail = np.clip(self._overlap_tail, -32768.0, 32767.0).astype(np.int16).tobytes()
        self._overlap_tail.fill(0)
        self._buffer.clear()
        return tail

    # ---------------------------------------------------------------- processing
    def process(self, pcm: bytes) -> bytes:
        if not pcm:
            return pcm
        self._buffer.extend(pcm)
        output = bytearray()
        while len(self._buffer) >= self._frame_bytes:
            frame_bytes = bytes(self._buffer[: self._frame_bytes])
            frame_i16 = np.frombuffer(frame_bytes, dtype=np.int16)
            enhanced = self._enhance_frame(frame_i16, frame_bytes)
            emitted = self._overlap_add(enhanced)
            output.extend(emitted)
            del self._buffer[: self._hop_bytes]
        return bytes(output)

    # ---------------------------------------------------------------- internals
    def _overlap_add(self, frame: np.ndarray) -> bytes:
        frame = np.asarray(frame, dtype=np.float32)
        head = frame[: self._hop_samples] + self._overlap_tail[: self._hop_samples]
        tail = frame[self._hop_samples :]
        if tail.size != self._overlap_tail.size:
            # Should never happen but keeps us safe when env vars are mis-set.
            resized = np.zeros_like(self._overlap_tail)
            resized[: min(tail.size, resized.size)] = tail[: resized.size]
            self._overlap_tail = resized
        else:
            self._overlap_tail = tail
        head = np.clip(head, -32768.0, 32767.0)
        return head.astype(np.int16).tobytes()

    def _enhance_frame(self, frame_i16: np.ndarray, frame_bytes: bytes) -> np.ndarray:
        frame = frame_i16.astype(np.float32)
        frame_rms = math.sqrt(float(np.mean(np.square(frame)))) if frame.size else 0.0
        frame_peak = float(np.max(np.abs(frame))) if frame.size else 0.0
        speech_detected = self._is_speech(frame_bytes, frame_rms, frame_peak)
        windowed = frame * self._window
        spectrum = np.fft.rfft(windowed, n=self._fft_len)
        magnitude = np.abs(spectrum)
        noise_floor = self._update_noise_profile(magnitude, speech_detected)
        reduced = magnitude - (noise_floor * self._suppression_strength)
        gains = np.divide(
            np.maximum(reduced, 0.0),
            magnitude + self._eps,
            out=np.zeros_like(magnitude),
            where=magnitude > 0,
        )
        gain_floor = self._speech_gain_floor if speech_detected else self._silence_gain_floor
        gains = np.clip(gains, gain_floor, 1.0)
        gains = self._apply_band_controls(gains, magnitude, speech_detected)
        enhanced_spec = spectrum * gains
        enhanced = np.fft.irfft(enhanced_spec, n=self._fft_len)[: self._frame_samples]
        enhanced *= self._window
        enhanced = self._suppress_transient(enhanced, frame_rms, frame_peak)
        return enhanced

    def _update_noise_profile(self, magnitude: np.ndarray, speech_detected: bool) -> np.ndarray:
        if not speech_detected or self._frames_seen < 5:
            self._noise_profile = (
                self._noise_smoothing * self._noise_profile
                + (1.0 - self._noise_smoothing) * magnitude
            )
        else:
            leakage = 0.02
            self._noise_profile = (
                (1.0 - leakage) * self._noise_profile + leakage * magnitude
            )
        self._frames_seen += 1
        return np.maximum(self._noise_profile, self._eps)

    def _apply_band_controls(
        self, gains: np.ndarray, magnitude: np.ndarray, speech_detected: bool
    ) -> np.ndarray:
        if speech_detected:
            return gains
        for idx in range(self._band_count):
            start = self._band_edges[idx]
            end = self._band_edges[idx + 1]
            if end <= start:
                continue
            band_energy = float(np.mean(magnitude[start:end]))
            prev = self._band_noise[idx]
            self._band_noise[idx] = (
                self._band_smoothing * prev + (1.0 - self._band_smoothing) * max(band_energy, self._eps)
            )
            guard = self._band_noise[idx] * self._band_guard
            if band_energy < guard:
                attenuation = max(self._band_floor, band_energy / (guard + self._eps))
                gains[start:end] = np.minimum(gains[start:end], attenuation)
        return np.clip(gains, 0.0, 1.0)

    def _suppress_transient(self, frame: np.ndarray, rms: float, peak: float) -> np.ndarray:
        if rms <= 0.0:
            return frame
        crest = peak / (rms + self._eps) if rms else float("inf")
        if crest >= self._transient_crest_threshold and rms < self._noise_rms * self._transient_rms_ratio:
            self._transient_counter = min(self._transient_counter + 1, self._max_transient_frames)
            if self._transient_counter <= self._max_transient_frames:
                return np.zeros_like(frame)
        else:
            self._transient_counter = 0
        return frame

    def _is_speech(self, frame_bytes: bytes, rms: float, peak: float) -> bool:
        vad_speech = True
        if self._vad is not None:
            try:
                vad_speech = self._vad.is_speech(frame_bytes, self.sample_rate)
            except Exception as exc:  # pragma: no cover
                logger.debug("VAD failure, falling back to energy gate: %s", exc)
                vad_speech = True
        # Update adaptive noise floor when VAD says "not speech".
        if not vad_speech:
            self._noise_rms = 0.9 * self._noise_rms + 0.1 * rms

        # Click guard: impulsive spikes with low RMS are treated as noise.
        if peak >= self._click_guard_level and rms < self._noise_rms * 1.4:
            return False

        # Final speech check blends VAD output with adaptive thresholding.
        threshold = max(self._noise_rms * 1.8, 900.0)
        return vad_speech and (rms >= threshold)
