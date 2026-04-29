import base64
import uuid
from typing import Dict

from fastapi import WebSocket
from starlette.websockets import WebSocketState
from logger_config import get_logger
from services.event_emmiter import EventEmitter

logger = get_logger("Stream")


class StreamService(EventEmitter):
    def __init__(self, websocket: WebSocket):
        super().__init__()
        self.ws = websocket
        self.expected_audio_index = 0
        self.audio_buffer: Dict[int, str] = {}
        self.stream_sid = ''
        self._mark_interval = 10
        self._frames_since_mark = 0

    def set_stream_sid(self, stream_sid: str):
        self.stream_sid = stream_sid
        logger.info(f"Stream SID set to {self.stream_sid}")

    def _ensure_stream_ready(self) -> bool:
        if not self.stream_sid:
            logger.warning("Cannot send Twilio payload because streamSid has not been set.")
            return False
        if (
            self.ws.client_state != WebSocketState.CONNECTED
            or self.ws.application_state != WebSocketState.CONNECTED
        ):
            logger.debug("WebSocket not connected; skipping audio send.")
            return False
        return True

    async def buffer(self, index: int, audio: str):
        """
        Buffer audio for playback, ensuring it is played in the correct order.
        """
        logger.debug(f"Buffering audio at index {index} for streamSid {self.stream_sid}")
        if index is None:  # No index means immediate audio playback
            await self.send_audio(audio)
            return

        if index == self.expected_audio_index:
            await self.send_audio(audio)
            self.expected_audio_index += 1

            # Flush buffered audio in order
            while self.expected_audio_index in self.audio_buffer:
                buffered_audio = self.audio_buffer[self.expected_audio_index]
                await self.send_audio(buffered_audio)
                del self.audio_buffer[self.expected_audio_index]
                self.expected_audio_index += 1
        else:
            # Store out-of-order audio in the buffer
            self.audio_buffer[index] = audio

    def stop_playback(self):
        """
        Stop playback immediately by clearing the audio buffer and resetting the state.
        """
        logger.info(f"Stopping playback and clearing audio buffer for streamSid {self.stream_sid}")
        logger.info(f"Before stopping playback: expected_index={self.expected_audio_index}, buffer_size={len(self.audio_buffer)}")
        self.expected_audio_index = 0
        self.audio_buffer = {}
        self._frames_since_mark = 0
        logger.info("Playback stopped and audio buffer cleared.")

    def reset(self):
        """
        Reset the stream by stopping playback and clearing the buffer.
        """
        self.stop_playback()
        logger.info(f"StreamService reset: Cleared audio buffer and reset expected index.")

    async def send_audio(self, audio: str):
        """
        Send audio data to Twilio's media stream via WebSocket.
        """
        if not audio:
            logger.warning("Attempted to send empty audio payload; dropping frame.")
            return
        if not self._ensure_stream_ready():
            logger.debug("Dropping audio payload because streamSid is missing.")
            return
        try:
            raw_bytes = base64.b64decode(audio, validate=True)
        except Exception as exc:
            logger.error(f"Audio payload is not valid base64; dropping frame: {exc}")
            return

        payload = {
            "streamSid": self.stream_sid,
            "event": "media",
            "media": {
                "payload": audio
            }
        }

        try:
            logger.debug(
                "Sending audio for streamSid %s (%d bytes).",
                self.stream_sid,
                len(raw_bytes)
            )
            await self.ws.send_json(payload)

            if self._mark_interval:
                self._frames_since_mark += 1
                if self._frames_since_mark >= self._mark_interval:
                    mark_label = str(uuid.uuid4())
                    if not self._ensure_stream_ready():
                        return
                    mark_payload = {
                        "streamSid": self.stream_sid,
                        "event": "mark",
                        "mark": {
                            "name": mark_label
                        }
                    }
                    logger.debug(
                        "Sending mark for streamSid %s, mark: %s",
                        self.stream_sid,
                        mark_label
                    )
                    await self.ws.send_json(mark_payload)
                    self._frames_since_mark = 0

                    # Emit an event when audio is sent
                    await self.emit('audiosent', mark_label)
        except Exception as e:
            logger.error(f"Error sending audio to WebSocket: {str(e)}")
            await self.emit('error', str(e))  # Optional: Emit an error event
