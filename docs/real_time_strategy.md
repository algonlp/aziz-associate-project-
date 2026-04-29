## Real-Time Response Strategy

These notes document the plan that was implemented to keep agent responses
under 500 ms without changing any of the higher-level product workflows.

### Response-Time Goals

1. **Sub–500 ms audible response:** the first assistant audio frame must be on
   the wire within half a second of the user finishing their turn.
2. **No regression to existing behaviours:** the latency optimisations must be
   opt-in configurable and keep message quality intact.
3. **Actionable observability:** every interaction should log whether the
   target was met.

### Implementation Highlights

| Lever | Purpose | Code Hook |
|-------|---------|-----------|
| Faster LLM chunk thresholds | Emit partial replies after ~1–2 tokens by lowering `LLM_MIN_CHARS`, `LLM_MAX_CHARS`, and `LLM_MAX_DELAY`. | `services/llm_service.py` |
| Latency budget + forced flush | Start a timer when the first tokens arrive; if no sentence boundary is seen before `LLM_RESPONSE_BUDGET_SEC` (default 450 ms) the pending text is flushed verbatim so TTS can start. | `AbstractLLMService.emit_complete_sentences` |
| First-response telemetry | Record the delta between the transcription timestamp and the first emitted assistant chunk. Warnings are logged when the 500 ms target is breached. | `AbstractLLMService._emit_sentence` |

These changes are purely algorithmic; no behavioural knobs move unless the new
environment variables are overridden.

## Answer-First + Ack Suppression

Two production guards keep the agent direct and prevent repeated "Okay" loops:

1. **Answer-first scheduler.** When a caller asks for pricing, any price-bearing
   sentence is emitted first (with a short wait window controlled by
   `ANSWER_FIRST_PRICE_WAIT_SEC`) before follow-up context is spoken.
2. **Acknowledgement gating.** Fast-lane and final acknowledgements are
   suppressed for questions, rate-limited via `ACK_COOLDOWN_SEC`, and canceled
   as soon as the first LLM chunk arrives. This keeps latency masking without
   audible "ok ok" spam.

## Production Interruption Strategy

Interruption handling also received a production-ready architecture pass:

1. **Single gate for playback teardown.** An `asyncio.Lock` ensures that
   overlapping ElevenLabs partial-transcript callbacks cannot try to cancel
   playback twice, which previously led to race conditions.
2. **Deterministic cancellation flow.** A helper now stops the filler audio,
   clears the WebSocket buffer, and cancels every pending TTS coroutine inside
   the lock before letting the LLM resume.
3. **Debounced user intent.** Interruptions triggered within the
   `INTERRUPT_DEBOUNCE_SEC` window (default 0.5 s) are ignored, preventing
   repeated stop/start storms caused by interim transcripts.

Because all of the cleanup is isolated to the new helper, the rest of the
application does not need to know about the low-level details of stopping
playback: existing business logic remains untouched.

## Listener-First Conversation Guard

ElevenLabs sometimes emits closely spaced partial/final payloads for the same
user utterance, which previously caused the agent to repeat itself or talk over
the caller.  The WebSocket loop now keeps a short history of the most recent final
transcript; if the same text arrives within
`TRANSCRIPT_DUPLICATE_WINDOW_SEC` (default 2s) the LLM turn is skipped.  To
avoid echoing ourselves, assistant audio chunks are also suppressed when the
same sentence comes back-to-back within `ASSISTANT_DUPLICATE_WINDOW_SEC`
(default 1.5s).  Together these guards ensure that we only respond to genuinely
new content while still allowing humans to repeat themselves after a short
pause.

Together with the `INTERRUPT_DEBOUNCE_SEC` gate and the playback teardown lock,
the agent now yields immediately when a human starts speaking and never restarts
until fresh audio is received.

## Noise-Resilient Barge-In

Background speech and short acknowledgements can trigger unwanted barge-ins. A
lightweight guard now ignores short tokens listed in
`USER_BARGE_IGNORE_TOKENS` (e.g., "yeah", "ok"), while still honoring explicit
interrupt phrases from `USER_BARGE_FORCE_PHRASES` (e.g., "stop", "hold on").
This keeps the agent speaking smoothly without abrupt mid-sentence halts.

## Fast Transcript Flush

Waiting for server-side `speech_final` flags to arrive could add several seconds
when callers paused mid-sentence.  A lightweight timer now monitors the buffered
final transcript and, if no new audio arrives within
`TRANSCRIPT_FLUSH_SEC` (default 200 ms), forces a `transcription` emit so the
LLM and TTS can respond immediately.  The next ElevenLabs final simply starts a
new buffer, so conversational accuracy is preserved while latency drops under
the 500 ms requirement.

## ElevenLabs Realtime STT Strategy

Per ElevenLabs’ [Scribe v2 Realtime documentation](https://elevenlabs.io/docs/api-reference/speech-to-text/v-1-speech-to-text-realtime),
the most reliable way to hit sub–500 ms turns is to feed 16 kHz PCM chunks,
enable the platform’s VAD commit strategy, and keep chunk sizes below 100 ms:

- Twilio delivers 8 kHz mu-law frames, so the new `TranscriptionService`
  decodes to PCM16 and upsamples to 16 kHz before sending each chunk.
- The WebSocket is provisioned with `AudioFormat.PCM_16000`, `CommitStrategy.VAD`,
  and the recommended VAD thresholds (`vad_threshold=0.4`,
  `vad_silence_threshold=0.6`, `min_speech_duration_ms = min_silence_duration_ms = 120`).
- Language hints from the inbound call (`en-US`, `sv`, etc.) are mapped to ISO
  codes, which the ElevenLabs docs call out as a best practice for accuracy.
- The `TranscriptionService` now automatically reconnects (`ELEVENLABS_STT_MAX_RECONNECTS`)
  with a brief backoff (`ELEVENLABS_STT_RECONNECT_DELAY_SEC`) whenever ElevenLabs tears
  down the session, preventing short-lived WebSocket hiccups from dropping the call.

This setup keeps the first assistant frame under 500 ms and matches the vendor
guidance without requiring any extra services or daemons.

## Operating the Changes

* The new latency parameters are optional environment variables and have
  conservative defaults that keep behaviour backward-compatible.
* No additional services are required: everything runs inside the existing
  FastAPI process.
* Logging now surfaces both response-time measurements and interruption
  lifecycle messages so you can track the system in production dashboards.

## Lag Regression Strategy

If you notice slower turn-taking, use this checklist to bring latency back under
500ms without changing product behavior:

1. **Force early LLM flushes.** Set `LLM_FORCE_FLUSH_ON_BUDGET=true` if you can
   tolerate mid-sentence flushes in exchange for faster first audio. Leave it
   false to preserve natural sentence boundaries.
2. **Shorter STT chunks.** Lower `TWILIO_STT_CHUNK_MS` to 60ms (default) or below
   so ElevenLabs receives smaller frames more frequently.
3. **Faster STT commit.** Ensure `ELEVENLABS_STT_MIN_SILENCE_MS=120` and
   `ELEVENLABS_STT_MIN_SPEECH_MS=120` so final transcripts do not wait on long
   silence gaps.
4. **Streaming TTS.** Keep `ELEVENLABS_STREAM_LATENCY` low (0-1) and avoid large
   `TTS_FRAME_MS` values so audio begins quickly.
5. **Monitor first-audio metrics.** The server logs warn when the first audio
   chunk exceeds the budget. Use those alerts to confirm improvements.
