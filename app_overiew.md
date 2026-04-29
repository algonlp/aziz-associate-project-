# App Overview

This document explains the current codebase for the AssociateAI calling platform so a new
full-stack developer can onboard quickly. It covers architecture, key flows, major
modules, data storage, external integrations, and configuration.

Repository root: `/root/aziz-associate`

## 1) What the app does
- Runs AI-driven voice calls (inbound + outbound) through Twilio Media Streams.
- Uses ElevenLabs for real-time STT (speech-to-text) and streaming TTS (text-to-speech).
- Uses OpenAI (and optional Gemini) for the LLM to generate responses and call actions.
- Supports bulk calling campaigns from CSV uploads.
- Tracks call and campaign stats, saves transcripts, and can send lead summary emails.

## 2) High-level architecture

Frontend
- Static UI: `statics/index.html` + JS/CSS.
- Legacy Streamlit UI (likely outdated): `ui/streamlit_app.py`.

Backend
- FastAPI app in `app.py`.
- WebSocket endpoint `/connection` for real-time audio streaming.
- REST endpoints for agents, calls, CSV/campaigns, transcripts, and settings.

Services (core runtime)
- `services/transcription_service.py`: ElevenLabs STT (Scribe realtime stream).
- `services/tts_service.py`: ElevenLabs TTS streaming with Redis cache.
- `services/llm_service.py`: OpenAI/Gemini chat LLMs.
- `services/stream_service.py`: Twilio audio streaming (outbound audio).
- `services/call_context.py`: per-call state container.
- `services/call_context_storage.py` + `services/context_storage.py`: Redis-backed context.
- `services/campaign_store.py`: Redis counters for campaign metrics.
- `services/lead_summary_service.py`: structured lead summary for email.
- `services/email_service.py`: SMTP email sender.
- `services/prompt_repository.py`: global system prompt file.
- `services/filler_audio_service.py`: optional filler audio for “please wait”.

Functions (LLM tool calls)
- `functions/function_manifest.py`: tool definitions.
- `functions/transfer_call.py`: ARI/AMI/Twilio transfer logic.
- `functions/end_call.py`: end call flow.
- `functions/book_appointment.py`: placeholder appointment storage (see tech debt).

External systems
- Twilio (voice + Media Streams)
- ElevenLabs (STT + TTS)
- OpenAI (LLM)
- Asterisk ARI/AMI (call transfer)
- Redis (caching + call/campaign state)
- SMTP (lead email)

## 3) End-to-end call pipeline

Inbound or outbound call
1) Twilio starts a call and opens a Media Stream to `/connection`.
2) Backend receives WebSocket audio frames (Twilio mu-law 8k).
3) `transcription_service` converts to PCM 16k and streams to ElevenLabs STT.
4) Final transcripts are sent to `llm_service`.
5) `llm_service` generates responses and may call tools (transfer, end call, etc).
6) `tts_service` streams ElevenLabs TTS (ulaw_8000) back through `stream_service`.
7) Twilio plays audio to the caller. Interruptions stop playback and reset buffers.

Latency measurement
- LLM timing is tracked in logs (first audio chunk vs target budget).
- Key tunables are under LLM/STT/TTS env variables (see Section 8).

## 4) Call modes

Outbound single call
- `/start_call` creates a Twilio call to a lead and uses agent settings.
- First sentence is sent immediately after stream start.

Inbound call
- `/incoming` returns TwiML for inbound numbers.
- Uses `inbound_config.json` or agent mapping if configured.

Bulk campaign
- `/upload_csv/` accepts a CSV of leads.
- `/start_bulk_calls` triggers multiple outbound calls.
- `campaign_store` tracks counts: initiated, completed, success, declined, failed.
- `/campaign_status/{id}` exposes status for UI.

## 5) Agent configuration and prompts

Agent configs are stored in `prompt_cache.json`.
Important fields:
- `id`, `name`, `avatar`
- `prompt` (full script and logic)
- `first_sentence` (initial greeting)
- `language`, `voice`
- `from_number`, `transfer_number`
- `agent_type` (inbound/outbound)
- `email_tool` (enable lead email)

Global system prompt lives in `system_prompt.txt` and is editable from Settings.

Note: The app also reads `inbound_config.json` for default inbound behavior.

## 6) WebSocket endpoint

`/connection` in `app.py` handles:
- Twilio "start" event
- audio frames
- barge-in detection
- TTS playback streaming
- clean disconnect

Audio flow:
- inbound frames -> STT
- LLM output -> TTS -> Twilio
- playback halts on user interruption to reduce overlap

## 7) REST API overview

Auth/UI
- `GET /login`, `POST /login`, `GET /logout`
- `GET /` (redirect)
- `GET /calling` (main UI)
- `GET /api/protected`

Settings
- `POST /update_user_settings`
- `GET /get_user_settings`
- `POST /update_twilio_message`
- `GET /get_twilio_message`
- `GET /get_twilio_numbers`

Agents/prompts
- `GET /fetch_prompts`
- `POST /add_prompt`
- `DELETE /delete_prompt/{id}`
- `POST /update_prompt/{id}`
- `POST /update_agent`
- `GET /agent/{twilio_number}`
- `GET /get_inbound_settings`
- `POST /update_inbound_settings`

Calls
- `POST /start_call`
- `POST /incoming` (TwiML)
- `GET /call_status/{sid}`
- `POST /end_call_status`
- `POST /incoming/recording-status`
- `GET /call_recording/{sid}`
- `GET /check_transcription/{recording_sid}`

Campaigns + CSV
- `POST /upload_csv/`
- `GET /current_csv/`
- `GET /read_csv/`
- `POST /start_bulk_calls`
- `GET /campaign_status/{id}`
- `GET /dashboard_stats`
- `GET /calling_stats`

Transfers
- `POST /save_transfer_number`
- `GET /get_transfer_numbers`
- `DELETE /delete_transfer_number/{phone_number}`

Transcripts + summaries
- `GET /transcript/{sid}`
- `GET /fetch_transcript/{sid}`
- `GET /all_transcripts`
- `GET /generate_summary/{sid}`

ARI
- `POST /ari/channel` (Asterisk ARI events)

WebSocket
- `GET /connection`

## 8) Configuration and environment variables

Twilio
- `TWILIO_ACCOUNT_SID`
- `TWILIO_AUTH_TOKEN`
- `TWILIO_CALLER_ID` (if set)

OpenAI / Gemini
- `OPENAI_API_KEY`
- `OPENAI_MODEL` (default gpt-5-mini)
- `GEMINI_API_KEY`, `GEMINI_MODEL`

ElevenLabs
- `ELEVENLABS_API_KEY`
- `ELEVENLABS_VOICE_ID` (default voice)
- `ELEVENLABS_MODEL_ID`
- `ELEVENLABS_STT_MODEL_ID` (scribe v2 realtime)
- `ELEVENLABS_STT_LANGUAGE`
- `ELEVENLABS_STT_TIMEOUT`
- `ELEVENLABS_STREAM_LATENCY`
- `ELEVENLABS_TIMEOUT`

SMTP email
- `EMAIL_SENDER`
- `EMAIL_PASSWORD`
- `EMAIL_SMTP_HOST`
- `EMAIL_SMTP_PORT`
- `EMAIL_SMTP_USE_SSL`
- `EMAIL_RECIPIENT_DEFAULT`

Redis
- `REDIS_URL`

LLM + latency controls
- `LLM_RESPONSE_BUDGET_SEC`
- `LLM_MIN_CHARS`
- `LLM_MAX_CHARS`
- `LLM_FLUSH_MIN_SEC`
- `LLM_FLUSH_MAX_SEC`
- `LLM_STREAM_CHUNK_WORDS`
- `LLM_TARGET_FIRST_AUDIO_SEC`

STT / audio tuning
- `TRANSCRIPT_FLUSH_SEC`
- `TWILIO_STT_CHUNK_MS`
- `USER_SPEECH_RMS_THRESHOLD`
- `ENABLE_NOISE_SUPPRESSION`
- `NOISE_FRAME_MS`, `NOISE_HOP_MS`, `NOISE_STRENGTH`

TTS
- `TTS_FRAME_MS`

Other
- `SYSTEM_MESSAGE` (fallback system prompt)
- `TRANSFER_NUMBER`

Settings UI writes allowed keys to `.env` and updates `system_prompt.txt`.

## 9) Data and storage

Redis
- Call context state (current conversation, flags, stream id, etc).
- Campaign counters.
- TTS cache (`tts:<voice_id>:<text>`).

File storage
- `prompt_cache.json` (agents)
- `inbound_config.json` (inbound defaults)
- `transfernumbers.json` (saved transfer numbers)
- `twilio_message.json` (shared message)
- `system_prompt.txt` (system prompt)
- `conversation_logs/` (per-call transcripts)
- `uploads/` (CSV files)

## 10) UI pages (statics/index.html)

Main sections:
- Dashboard (recent stats)
- Agents (create/edit agents + prompts)
- Bulk Call (upload CSV, start campaign)
- Call Logs
- Transcripts
- Voice Clone (UI only; backend wiring incomplete)
- Settings (Twilio/OpenAI/ElevenLabs/Email/System Prompt)

## 11) Lead email flow

When enabled:
- Lead summary is generated in `lead_summary_service.py`.
- `email_service.py` sends summary + key fields via SMTP.
- Triggered when the model detects lead interest, or via post-call summary.

## 12) Transfer flow

Tools
- `transfer_call` is a tool callable by LLM.

Logic
- Tries ARI transfer first, then AMI redirect, then Twilio transfer fallback.
- Transfer destination is taken from agent config or `TRANSFER_NUMBER`.

## 13) Known gaps / tech debt

High priority
- `voice_clone.py` contains a hardcoded API key (security risk).
- Voice clone endpoints are not mounted in `app.py` but UI calls `/voice/clone`.
- `functions/book_appointment.py` writes to `/root/aziz_app/...` (path mismatch).

Behavioral tuning
- STT VAD uses spectral gating if `webrtcvad` is missing.
- Some LLM outputs are fragmented into short sentences; tuning may be needed.

Operational
- `.env` is modified at runtime; ensure secure handling in production.

## 14) Logs and observability

Common log patterns
- `Interaction X STT -> LLM` with transcription text.
- `Interaction X LLM -> TTS` with assistant chunks.
- `first audio chunk at ... ms` (latency KPI).
- `User interruption detected` (barge-in).
- `Ingress media stats` (frame/byte counts).

Transcripts
- Saved in `conversation_logs/` and exposed via `/transcripts`.

## 15) How to run (dev)

Typical commands
- `pip install -r requirements.txt`
- `uvicorn app:app --host 0.0.0.0 --port 8000`

Production (current behavior based on logs)
- Gunicorn with Uvicorn workers.

## 16) Key files and where to edit

Core runtime
- `app.py` (routes + websocket)
- `services/*` (STT/TTS/LLM pipeline)
- `functions/*` (transfer + call tools)

Configuration and data
- `prompt_cache.json`, `inbound_config.json`
- `system_prompt.txt`
- `twilio_message.json`, `transfernumbers.json`

UI
- `statics/index.html`
- `statics/login.html`

Docs
- `docs/real_time_strategy.md`
- `docs/noise_cancellation.md`
- `docs/openai_structured_output.md`
- `docs/twilio_call_limits.md`

---

If a developer needs to extend or debug the streaming call flow, start with:
1) `app.py` WebSocket handler (`/connection`)
2) `services/transcription_service.py`
3) `services/llm_service.py`
4) `services/tts_service.py`
5) `services/stream_service.py`
