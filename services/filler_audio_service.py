import asyncio
import audioop
import base64
import os
import random
import wave
from pathlib import Path
from typing import Dict, List, Optional

from logger_config import get_logger

logger = get_logger("FillerAudio")


def _load_audio_frames(path: Path, frame_ms: int, sample_rate: int) -> List[bytes]:
    if not path.exists():
        logger.warning(f"Filler audio file not found: {path}")
        return []

    try:
        with wave.open(str(path), "rb") as wav_file:
            channels = wav_file.getnchannels()
            sampwidth = wav_file.getsampwidth()
            framerate = wav_file.getframerate()
            raw_audio = wav_file.readframes(wav_file.getnframes())

        # Ensure mono
        if channels == 2:
            raw_audio = audioop.tomono(raw_audio, sampwidth, 0.5, 0.5)
        elif channels > 2:
            logger.warning("Filler audio has more than 2 channels; using the first two for downmix.")
            raw_audio = audioop.tomono(raw_audio, sampwidth, 1, 0)

        # Convert to 16-bit PCM if necessary
        if sampwidth != 2:
            raw_audio = audioop.lin2lin(raw_audio, sampwidth, 2)
            sampwidth = 2

        # Resample to target rate
        if framerate != sample_rate:
            raw_audio, _ = audioop.ratecv(raw_audio, sampwidth, 1, framerate, sample_rate, None)

        # Convert to mu-law
        mulaw_audio = audioop.lin2ulaw(raw_audio, sampwidth)

        frame_bytes = int(sample_rate * frame_ms / 1000)
        frames = [
            mulaw_audio[i:i + frame_bytes]
            for i in range(0, len(mulaw_audio), frame_bytes)
            if len(mulaw_audio[i:i + frame_bytes]) == frame_bytes
        ]
        logger.info(f"Loaded {len(frames)} filler frames from {path}")
        return frames
    except Exception as exc:
        logger.error(f"Failed to load filler audio {path}: {exc}")
        return []


class FillerAudioPlayer:
    def __init__(self, stream_service):
        self.stream_service = stream_service
        self.sample_rate = 8000
        self.frame_ms = int(os.getenv("FILLER_FRAME_MS", "40"))
        fallback_path = Path(os.getenv("FILLER_AUDIO_PATH", "audio/slow_typing.wav"))
        self.audio_dir = Path(os.getenv("FILLER_AUDIO_DIR", fallback_path.parent if fallback_path.parent else "audio"))
        self.audio_paths = self._discover_audio_paths(fallback_path)
        self._frames_cache: Dict[Path, List[bytes]] = {}
        self._active_frames: List[bytes] = []
        self._active_audio: Optional[Path] = None
        self.frame_delay = self.frame_ms / 1000.0
        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._lock = asyncio.Lock()

    @property
    def has_audio(self) -> bool:
        return any(path.exists() for path in self.audio_paths)

    def _discover_audio_paths(self, fallback_path: Path) -> List[Path]:
        candidates: List[Path] = []
        if self.audio_dir.exists():
            for item in sorted(self.audio_dir.iterdir()):
                if item.is_file() and item.suffix.lower() in {".wav", ".wave"}:
                    candidates.append(item)
        if fallback_path.exists() and fallback_path not in candidates:
            candidates.append(fallback_path)
        if not candidates:
            logger.warning(f"No filler audio files found in {self.audio_dir}.")
        return candidates

    def _get_frames_for_path(self, path: Path) -> List[bytes]:
        if path not in self._frames_cache:
            self._frames_cache[path] = _load_audio_frames(path, self.frame_ms, self.sample_rate)
        return self._frames_cache[path]

    def _select_audio_frames(self) -> List[bytes]:
        available = [path for path in self.audio_paths if path.exists()]
        if not available:
            logger.warning("No available filler audio files to select from.")
            return []

        choices = [path for path in available if path != self._active_audio]
        target_path = random.choice(choices or available)
        frames = self._get_frames_for_path(target_path)

        if not frames:
            logger.warning(f"Selected filler audio {target_path} has no frames. Trying another file.")
            remaining = [path for path in available if path != target_path]
            if not remaining:
                return []
            target_path = random.choice(remaining)
            frames = self._get_frames_for_path(target_path)

        if frames:
            self._active_audio = target_path
            logger.info(f"Selected filler audio file: {target_path.name}")
        return frames

    async def start(self):
        async with self._lock:
            if self._task or not self.has_audio:
                return
            frames = self._select_audio_frames()
            if not frames:
                return
            self._active_frames = frames
            self._stop_event = asyncio.Event()
            self._task = asyncio.create_task(self._run())
            logger.info("Started filler audio playback.")

    async def stop(self):
        async with self._lock:
            task = self._task
            if not task:
                return
            if self._stop_event:
                self._stop_event.set()
            self._task = None
            self._stop_event = None

        try:
            await task
        except asyncio.CancelledError:
            pass
        logger.info("Stopped filler audio playback.")

    async def reset(self):
        await self.stop()

    async def _run(self):
        frames = self._active_frames
        if not frames:
            logger.warning("No filler frames available for playback.")
            return

        frame_index = 0
        try:
            while self._stop_event and not self._stop_event.is_set():
                payload = base64.b64encode(frames[frame_index]).decode("utf-8")
                await self.stream_service.buffer(None, payload)
                frame_index = (frame_index + 1) % len(frames)
                await asyncio.sleep(self.frame_delay)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"Filler audio playback error: {exc}")
