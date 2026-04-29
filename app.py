import asyncio
import io
import re
import base64
import hashlib
import json
import os
import uuid
import unicodedata
from pathlib import Path as FsPath
from collections import deque
from typing import Any, Dict, Iterable, List, Literal, Optional, Set, Tuple
import functools
from difflib import SequenceMatcher

import aiofiles
import aiohttp
import dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Form
from fastapi import FastAPI, HTTPException, Request  # Correct import for Request
from fastapi.responses import HTMLResponse, FileResponse
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
from twilio.twiml.voice_response import Connect, VoiceResponse

from logger_config import get_logger
from services.call_context import CallContext
from services.redis_client import get_redis_client
from services.tts_service import TTSFactory
from services.filler_audio_service import FillerAudioPlayer
from services.prompt_repository import read_global_prompt, write_global_prompt
from services.text_utils import (
    clean_alnum_space,
    extract_numbers,
    extract_words,
    find_word_index,
    has_digit,
    is_all_caps_heading,
    is_numbered_list_item,
    normalize_whitespace,
    remove_comma_before_punct,
    replace_insensitive,
    split_on_first_dash,
    starts_with_word,
    strip_leading_symbols,
    strip_non_digits,
    strip_space_before_punct,
    strip_trailing_word_and_punct,
)
from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI, UploadFile, File
import shutil
import csv
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
import pytz
import requests
from starlette.responses import StreamingResponse
from fastapi.responses import RedirectResponse
from fastapi.responses import FileResponse
from openai import OpenAI, AsyncOpenAI
import httpx  # An async HTTP client
import websockets

from pipecat.frames.frames import (
    AggregatedTextFrame,
    Frame,
    InterruptionFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    LLMTextFrame,
    InterimTranscriptionFrame,
    StartFrame,
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
    TTSAudioRawFrame,
    TTSSpeakFrame,
    TTSTextFrame,
    TextFrame,
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
    EmulateUserStartedSpeakingFrame,
    EmulateUserStoppedSpeakingFrame,
    VADUserStartedSpeakingFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.pipeline.base_task import PipelineTaskParams
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
    LLMAssistantAggregatorParams,
)
from pipecat.processors.aggregators.llm_text_processor import LLMTextProcessor
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection
from pipecat.serializers.twilio import TwilioFrameSerializer
from pipecat.services.llm_service import FunctionCallParams
from pipecat.services.openai.base_llm import BaseOpenAILLMService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.services.elevenlabs.tts import ElevenLabsTTSService
from pipecat.transports.websocket.fastapi import FastAPIWebsocketParams, FastAPIWebsocketTransport
from pipecat.utils.text.simple_text_aggregator import SimpleTextAggregator
from pipecat.utils.text.base_text_aggregator import Aggregation, AggregationType, BaseTextAggregator

from functions.function_manifest import tools as function_tools
from services.elevenlabs_realtime_stt import ElevenLabsRealtimeSTTService
from functions.transfer_call import transfer_call as transfer_call_func
from functions.end_call import end_call as end_call_func

from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
import pytz
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from jose import jwt, JWTError

from fastapi import Body
from passlib.context import CryptContext
from pydantic import BaseModel
from fastapi.templating import Jinja2Templates
from fastapi import Query
from passlib.context import CryptContext
import sys
import signal
import logging

from urllib.parse import urljoin

import hmac
import hashlib
import time
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from typing import Optional
from pydantic import BaseModel
from services.call_context_storage import save_call_context

_OPENAI_MODEL_CACHE: Optional[Set[str]] = None
_OPENAI_MODEL_CACHE_TS: float = 0.0
from services.lead_summary_service import summarize_lead
from services.email_service import send_email
from services.campaign_store import (
    create_campaign,
    get_campaign,
    increment_campaign,
    list_campaigns,
    list_campaign_contacts,
    link_call_to_contact,
    mark_call_finalized,
    mark_call_transferred,
    set_campaign_fields,
    update_campaign_contact_by_call_sid,
    upsert_campaign_contact,
)
from fastapi import Path, Body




app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or specify the exact Ngrok URL to be safe
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Ensure the correct .env file is loaded and override any existing values
dotenv.load_dotenv(override=True)

CALL_THROTTLE_SEC = float(os.getenv("TWILIO_CALL_THROTTLE_SEC", "1.0"))
TWILIO_CALL_RETRY_MAX = int(os.getenv("TWILIO_CALL_RETRY_MAX", "3"))
TWILIO_CALL_RETRY_BASE_SEC = float(os.getenv("TWILIO_CALL_RETRY_BASE_SEC", "0.8"))

_twilio_rate_lock = asyncio.Lock()
_twilio_last_call_at = 0.0


async def _wait_for_twilio_slot() -> None:
    global _twilio_last_call_at
    if CALL_THROTTLE_SEC <= 0:
        return
    async with _twilio_rate_lock:
        now = time.monotonic()
        elapsed = now - _twilio_last_call_at
        if elapsed < CALL_THROTTLE_SEC:
            await asyncio.sleep(CALL_THROTTLE_SEC - elapsed)
        _twilio_last_call_at = time.monotonic()


async def _create_twilio_call(client: Client, call_kwargs: Dict[str, Any]):
    last_exc: Optional[Exception] = None
    for attempt in range(max(TWILIO_CALL_RETRY_MAX, 1)):
        await _wait_for_twilio_slot()
        try:
            return await asyncio.to_thread(client.calls.create, **call_kwargs)
        except TwilioRestException as exc:
            last_exc = exc
            status = getattr(exc, "status", None)
            if status in (429, 500, 502, 503, 504):
                await asyncio.sleep(TWILIO_CALL_RETRY_BASE_SEC * (2 ** attempt))
                continue
            raise
        except Exception as exc:
            last_exc = exc
            await asyncio.sleep(TWILIO_CALL_RETRY_BASE_SEC * (2 ** attempt))
    if last_exc:
        raise last_exc


def _start_call_recording(call_sid: str, callback_url: str) -> None:
    if not call_sid:
        return
    try:
        client = get_twilio_client()
        client.calls(call_sid).recordings.create(
            recording_status_callback=callback_url,
            recording_status_callback_method="POST",
            recording_status_callback_event=["completed"],
        )
        logger.debug("Recording started for CallSid {}", call_sid)
    except Exception as exc:
        logger.warning("Failed to start recording for {}: {}", call_sid, exc)

# Allowed UI-modifiable keys
ALLOWED_KEYS = {
    "TWILIO_ACCOUNT_SID",
    "TWILIO_AUTH_TOKEN",
    "OPENAI_API_KEY",
    "ELEVENLABS_API_KEY",
    "TRANSFER_NUMBER",
    "EMAIL_SENDER",
    "EMAIL_RECIPIENT_DEFAULT",
}

BASE_DIR = FsPath(os.getenv("APP_BASE_DIR", str(FsPath(__file__).resolve().parent)))
ENV_FILE_PATH = os.getenv("ENV_FILE_PATH", str(BASE_DIR / ".env"))

class UserSettings(BaseModel):
    twilio_sid: str
    twilio_token: str
    openai_key: str
    elevenlabs_key: str
    system_message: Optional[str] = ""
    transfer_number: Optional[str] = ""
    email_sender: Optional[str] = ""
    email_recipient: Optional[str] = ""

@app.post("/update_user_settings")
async def update_user_settings(settings: UserSettings):
    """Update only the allowed environment variables while keeping others unchanged."""
    try:
        # Read existing .env file while preserving formatting
        existing_vars = []
        multiline_key = None
        multiline_value = []

        if os.path.exists(ENV_FILE_PATH):
            with open(ENV_FILE_PATH, "r") as file:
                for line in file:
                    stripped_line = line.strip()

                    # Detect multiline SYSTEM_MESSAGE start
                    if stripped_line.startswith("SYSTEM_MESSAGE="):
                        multiline_key = "SYSTEM_MESSAGE"
                        multiline_value = [stripped_line]  # Store the first line
                    elif multiline_key:
                        # Continue storing SYSTEM_MESSAGE lines
                        multiline_value.append(stripped_line)
                        if stripped_line.endswith('"'):  # End of multiline
                            existing_vars.append("\n".join(multiline_value))
                            multiline_key = None
                    else:
                        existing_vars.append(stripped_line)

        # Update only the allowed keys
        updated_env = []
        for line in existing_vars:
            if "=" in line and not line.startswith("#"):  # Ignore comments
                key, value = line.split("=", 1)
                if key in ALLOWED_KEYS:
                    # Replace with new values from UI
                    if key == "TWILIO_ACCOUNT_SID":
                        value = settings.twilio_sid
                    elif key == "TWILIO_AUTH_TOKEN":
                        value = settings.twilio_token
                    elif key == "OPENAI_API_KEY":
                        value = settings.openai_key
                    elif key == "ELEVENLABS_API_KEY":
                        value = settings.elevenlabs_key
                    elif key == "TRANSFER_NUMBER":
                        value = settings.transfer_number or ""
                    elif key == "EMAIL_SENDER":
                        value = settings.email_sender or ""
                    elif key == "EMAIL_RECIPIENT_DEFAULT":
                        value = settings.email_recipient or ""
                updated_env.append(f"{key}={value}")
            else:
                updated_env.append(line)  # Keep comments or empty lines unchanged

        # Write back the updated .env file
        with open(ENV_FILE_PATH, "w") as file:
            file.write("\n".join(updated_env) + "\n")

        dotenv.load_dotenv(override=True)  # Reload environment variables
        if settings.system_message is not None:
            write_global_prompt(settings.system_message)

        return {"success": True, "message": "Environment variables updated successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating .env file: {str(e)}")


@app.get("/get_user_settings")
async def get_user_settings():
    """Fetch the current user settings from .env file."""
    try:
        dotenv.load_dotenv()
        prompt_text = read_global_prompt()
        return {
            "twilio_sid": os.getenv("TWILIO_ACCOUNT_SID", ""),
            "twilio_token": os.getenv("TWILIO_AUTH_TOKEN", ""),
            "openai_key": os.getenv("OPENAI_API_KEY", ""),
            "elevenlabs_key": os.getenv("ELEVENLABS_API_KEY", ""),
            "transfer_number": os.getenv("TRANSFER_NUMBER", ""),
            "email_sender": os.getenv("EMAIL_SENDER", ""),
            "email_recipient": os.getenv("EMAIL_RECIPIENT_DEFAULT", ""),
            "system_message": prompt_text,
            "system_prompt": prompt_text,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching settings: {str(e)}")
# Load authentication credentials from environment variables.
USERNAME = (os.getenv("USERNAME") or "").strip()
PASSWORD = (os.getenv("PASSWORD") or "").strip()


logger = get_logger("App")
if not USERNAME or not PASSWORD:
    logger.warning("USERNAME/PASSWORD not configured; login with credentials is disabled.")

GREETING_AUDIO_DIR = FsPath(
    os.getenv("GREETING_AUDIO_DIR", str(BASE_DIR / "audio" / "greetings"))
)
GREETING_AUDIO_DIR.mkdir(parents=True, exist_ok=True)

def _looks_like_system_prompt(text: str) -> bool:
    if not text:
        return False
    lower = text.strip().lower()
    if lower.startswith("you are ") and any(token in lower for token in ("rules:", "core rules", "conversation handling", "price lock")):
        return True
    if "core rules" in lower or "conversation handling" in lower:
        return True
    if "price lock" in lower or "use the agent prompt as the single source of truth" in lower:
        return True
    if "speak in clear" in lower and "no filler" in lower:
        return True
    if lower.startswith("role") and "objective" in lower:
        return True
    if lower.startswith("personality") or lower.startswith("tone"):
        return True
    if lower.startswith("context"):
        return True
    if "reference pronunciations" in lower:
        return True
    if lower.startswith("instructions") and "rules" in lower:
        return True
    if lower.startswith("conversation flow"):
        return True
    if lower.startswith("safety") or lower.startswith("escalation"):
        return True
    if lower.startswith("language"):
        return True
    return False


def _looks_like_intro(text: str) -> bool:
    if not text:
        return False
    lower = text.strip().lower()
    if _looks_like_rules_text(lower):
        return False
    return any(
        phrase in lower
        for phrase in (
            "my name is",
            "this is",
            "i am",
            "i'm",
            "calling from",
            "calling on behalf",
        )
    )


def _looks_like_rules_text(text: str) -> bool:
    if not text:
        return False
    lower = text.lower()
    if starts_with_word(text, "rules") or starts_with_word(text, "rule") or is_numbered_list_item(text):
        return True
    if lower.startswith("you are ") and any(token in lower for token in ("speak", "rules", "core rules", "conversation handling")):
        return True
    if "rules you must follow" in lower:
        return True
    if "start every phone interaction" in lower:
        return True
    if "you must follow" in lower:
        return True
    if "core rules" in lower or "conversation handling" in lower:
        return True
    if "conversation guardrails" in lower:
        return True
    return False



# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT configuration
SECRET_KEY = (os.getenv("JWT_SECRET") or os.getenv("SECRET_KEY") or "").strip()
if not SECRET_KEY:
    SECRET_KEY = hashlib.sha256(os.urandom(32)).hexdigest()
    logger.warning("JWT_SECRET is not set; generated ephemeral secret for this process.")
ALGORITHM = "HS256"

# global call_contexts
# call_contexts = {}

# Serve static files
# Set filesystem paths via environment for portability.
static_dir = os.getenv("STATIC_DIR", str(BASE_DIR / "statics"))
transcript_dir = os.getenv("TRANSCRIPTS_STATIC_DIR", str(BASE_DIR / "transcrip"))
audio_static_dir = os.getenv("AUDIO_STATIC_DIR", str(BASE_DIR / "audio"))

# Mount static files
app.mount("/static", StaticFiles(directory=static_dir), name="static")
app.mount("/audio", StaticFiles(directory=audio_static_dir), name="audio")


def authenticate_user(username: str, password: str):
    """Validate user credentials."""
    if not USERNAME or not PASSWORD:
        logger.error("Authentication failed: USERNAME/PASSWORD environment variables are not configured.")
        return None
    logger.info(f"Attempting authentication for username: {username}")

    # Debugging: Log both the provided username and the stored username
    logger.debug(f"Provided username: {username.strip()}, Stored username: {USERNAME}")

    # Check if the username matches
    if username.strip() != USERNAME:
        logger.error("Authentication failed: Invalid username")
        return None

    # Debugging: Log a masked version of the password
    logger.debug(f"Provided password: {password[:2]}*** (masked), Stored password: {PASSWORD[:2]}*** (masked)")

    # Check if the password matches
    if password.strip() != PASSWORD:
        logger.error("Authentication failed: Invalid password")
        return None

    logger.info("Authentication successful")
    return {"username": username}



def create_access_token(data: dict, expires_delta: timedelta = timedelta(hours=1)):
    """Create a JWT access token."""
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(request: Request):
    token = request.cookies.get("auth_token")
    if not token:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")  # Return username
    except jwt.ExpiredSignatureError:
        logger.warning("JWT expired")
        return None
    except JWTError as e:
        logger.error(f"JWT Error: {e}")
        return None



# Routes

templates = Jinja2Templates(directory=static_dir)  # Ensure your static_dir has login templates


async def _ari_get_channel_variable(
    client: httpx.AsyncClient,
    base_url: str,
    auth: tuple,
    channel_id: str,
    variable: str,
) -> Optional[str]:
    url = f"{base_url}/channels/{channel_id}/variable"
    try:
        response = await client.get(url, params={"variable": variable}, auth=auth)
    except httpx.HTTPError as exc:
        logger.warning("ARI variable fetch failed for channel {}: {}", channel_id, exc)
        return None
    if response.status_code != 200:
        return None
    payload = response.json() or {}
    return payload.get("value")


def _channel_name_matches_prefix(channel_name: str, prefix: str) -> bool:
    if not channel_name or not prefix:
        return False
    if channel_name.startswith(prefix):
        return True
    if "/" in channel_name:
        short = channel_name.split("/", 1)[1]
        if short.startswith(prefix):
            return True
    return False


async def _ari_event_listener():
    base_url = (os.getenv("ASTERISK_ARI_BASE_URL") or "").rstrip("/")
    username = os.getenv("ASTERISK_ARI_USERNAME", "")
    password = os.getenv("ASTERISK_ARI_PASSWORD", "")
    if not base_url or not username or not password:
        logger.warning("ARI listener disabled: missing ARI configuration.")
        return

    ws_base = base_url.replace("https://", "wss://").replace("http://", "ws://")
    app_name = os.getenv("ASTERISK_ARI_APP", "ai-app")
    ws_url = f"{ws_base}/events?app={app_name}&api_key={username}:{password}&subscribeAll=true"
    def _to_timeout(env_key: str, default: str) -> Optional[float]:
        raw = os.getenv(env_key, default)
        try:
            value = float(raw)
        except (TypeError, ValueError):
            value = float(default)
        if value <= 0:
            return None
        return value

    ping_interval = _to_timeout("ASTERISK_ARI_PING_INTERVAL_SEC", "20")
    ping_timeout = _to_timeout("ASTERISK_ARI_PING_TIMEOUT_SEC", "20")
    open_timeout = _to_timeout("ASTERISK_ARI_OPEN_TIMEOUT_SEC", "10")
    close_timeout = _to_timeout("ASTERISK_ARI_CLOSE_TIMEOUT_SEC", "5")
    if ping_interval is None:
        ping_timeout = None
    backoff = 1.0
    max_backoff = 30.0
    while True:
        try:
            async with websockets.connect(
                ws_url,
                ping_interval=ping_interval,
                ping_timeout=ping_timeout,
                open_timeout=open_timeout,
                close_timeout=close_timeout,
                max_queue=16,
            ) as websocket:
                logger.info("ARI event listener connected")
                backoff = 1.0
                async with httpx.AsyncClient(timeout=5.0) as client:
                    async for message in websocket:
                        try:
                            event = json.loads(message)
                        except json.JSONDecodeError:
                            continue
                        event_type = event.get("type")
                        if event_type not in ("ChannelCreated", "ChannelStateChange", "StasisStart"):
                            continue
                        channel = event.get("channel") or {}
                        channel_id = channel.get("id")
                        channel_name = channel.get("name") or ""
                        if not channel_id:
                            continue
                        call_sid = await _ari_get_channel_variable(
                            client,
                            base_url,
                            (username, password),
                            channel_id,
                            "TWILIO_CALL_SID",
                        )
                        if not call_sid:
                            continue
                        call_context = get_call_context(call_sid)
                        if not call_context:
                            call_context = CallContext()
                            call_context.call_sid = call_sid
                        prefer_prefix = os.getenv(
                            "ASTERISK_TRANSFER_CHANNEL_PREFIX",
                            "PJSIP/a1r-",
                        ).strip()
                        is_preferred = _channel_name_matches_prefix(channel_name, prefer_prefix)
                        should_store = is_preferred or not getattr(call_context, "ari_channel_id", None)
                        if should_store:
                            call_context.ari_channel_id = channel_id
                            save_call_context(call_sid, call_context)
                            call_contexts[call_sid] = call_context
                            for key, ctx in active_stream_contexts.items():
                                if key.startswith(f"{call_sid}_"):
                                    ctx.ari_channel_id = channel_id
                            logger.debug(
                                "Stored ARI channel id from {} (CallSid={}, ChannelId={}, Preferred={}, Name={})",
                                event_type,
                                call_sid,
                                channel_id,
                                is_preferred,
                                channel_name,
                            )
        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as exc:
            logger.warning("ARI event listener disconnected: {}", exc)
        except TimeoutError as exc:
            logger.warning("ARI event listener timeout: {}", exc)
        except Exception as exc:
            logger.exception("ARI event listener error: {}", exc)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, max_backoff)


@app.on_event("startup")
async def _startup_events():
    asyncio.create_task(_ari_event_listener())
    # Prewarm core pipeline dependencies to reduce first-turn latency.
    try:
        asyncio.create_task(_prewarm_elevenlabs_stt())
    except Exception:
        pass
    try:
        default_voice = os.getenv("ELEVENLABS_VOICE_ID", "").strip()
        if default_voice:
            asyncio.create_task(_prewarm_elevenlabs_tts(default_voice))
    except Exception:
        pass
    try:
        asyncio.create_task(_prewarm_openai_llm(os.getenv("OPENAI_MODEL", "")))
    except Exception:
        pass


@app.get("/login", response_class=HTMLResponse)
async def login_page(message: str = Query(None)):
    """Serve login page with an optional message."""
    login_path = os.path.join(static_dir, "login.html")
    if os.path.exists(login_path):
        return templates.TemplateResponse("login.html", {"request": {}, "message": message})
    return {"error": "login.html not found"}

class LoginRequest(BaseModel):
    username: str
    password: str

class AriChannelPayload(BaseModel):
    call_sid: str
    ari_channel_id: str

@app.post("/login")
async def login(data: LoginRequest):
    username = data.username
    password = data.password

    if not username or not password:
        raise HTTPException(status_code=422, detail="Username or password missing")

    user = authenticate_user(username, password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")

    token = create_access_token({"sub": username})
    response = RedirectResponse(url="/calling", status_code=303)
    response.set_cookie(key="auth_token", value=token, httponly=True)
    return response

@app.get("/logout")
async def logout():
    """Handle user logout."""
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(key="auth_token")
    return response



@app.get("/", response_class=RedirectResponse)
async def root(request: Request):
    """Redirect to login or calling based on authentication."""
    user = get_current_user(request)
    return "/calling" if user else "/login"


@app.get("/calling", response_class=HTMLResponse)
async def calling(request: Request):
    """Serve calling page (authentication required)."""
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    calling_path = os.path.join(static_dir, "index.html")
    if os.path.exists(calling_path):
        return FileResponse(calling_path)
    return {"error": "index.html not found"}


@app.get("/api/protected")
async def protected_api(request: Request):
    """Protected API route."""
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return {"message": f"Welcome {user}, you are authorized!"}

# @app.get("/")
# async def root():
#     # Define the path to the static index.html file
#     file_path = os.path.join("statics", "index.html")
#     return FileResponse(file_path)

# Correctly mount the "transcript" directory at "/transcripts"
app.mount("/transcripts", StaticFiles(directory=transcript_dir), name="transcripts")

redis_client = get_redis_client()

# Path to save uploaded CSV files.
UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", "./uploads/")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Global dictionary to store call contexts for each server instance (should be replaced with a database in production)
global call_contexts
call_contexts = {}
active_stream_contexts: Dict[str, CallContext] = {}
active_campaign_tasks: Dict[str, asyncio.Task] = {}
interaction_tool_choice: Dict[int, Dict[str, Any]] = {}


import os

# Directory to store conversation logs
LOG_DIR = os.getenv(
    "CONVERSATION_LOG_DIR",
    str((FsPath(__file__).resolve().parent / "conversation_logs")),
)
os.makedirs(LOG_DIR, exist_ok=True)

def save_to_log(call_sid, role, message):
    """
    Save a message to the log file for the specified call_sid.

    :param call_sid: The Twilio call SID.
    :param role: The role of the sender ('user' or 'assistant').
    :param message: The message content.
    """
    log_file_path = os.path.join(LOG_DIR, f"{call_sid}.txt")
    clean_message = str(message or "").replace("\n", " ").strip()
    with open(log_file_path, "a") as log_file:
        log_file.write(f"{role}: {clean_message}\n")
    if os.getenv("TERMINAL_TRANSCRIPT_LOG_ENABLED", "true").lower() == "true":
        logger.info("Transcript CallSid={} {}: {}", call_sid, role, clean_message)


# Function to delete existing files in the uploads folder
def clear_upload_folder():
    """Deletes all the files in the upload folder before saving a new one."""
    for filename in os.listdir(UPLOAD_FOLDER):
        file_path = os.path.join(UPLOAD_FOLDER, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.remove(file_path)
            else:
                print(f"{file_path} is not a file or a symlink.")
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")

# Route to upload CSV file
@app.post("/upload_csv/")
async def upload_csv(file: UploadFile = File(...)):
    # Clear the folder first to remove any existing file
    clear_upload_folder()

    # Save the new file with the original filename
    file_location = os.path.join(UPLOAD_FOLDER, file.filename)
    with open(file_location, "wb") as f:
        shutil.copyfileobj(file.file, f)

    return {"message": "File uploaded successfully", "filename": file.filename}


# Route to check if a CSV file exists and return its name
@app.get("/current_csv/")
async def get_current_csv():
    files = os.listdir(UPLOAD_FOLDER)
    
    if files:
        # Return the first file found in the folder (since we only allow one at a time)
        return {"file_exists": True, "filename": files[0]}
    else:
        return {"file_exists": False}

# Route to read the contents of the current CSV file and return the count of numbers
@app.get("/read_csv/")
async def read_csv():
    files = os.listdir(UPLOAD_FOLDER)
    
    if files:
        file_path = os.path.join(UPLOAD_FOLDER, files[0])
        contacts = []
        with open(file_path, "r") as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                if len(row) >= 2:  # Ensure there's a name and phone number
                    contacts.append({"name": row[0], "phone_number": row[1]})
        
        total_contacts = len(contacts)
        return {
            "file_name": files[0], 
            "contacts": contacts, 
            "total_contacts": total_contacts
        }
    else:
        return {"error": "No file found"}


# def save_call_context(call_sid, call_context):
#     context_dict = {
#         "system_message": getattr(call_context, "system_message", None),
#         "initial_message": getattr(call_context, "initial_message", None),
#         "call_sid": getattr(call_context, "call_sid", None),
#         "user_context": getattr(call_context, "user_context", None),
#         "transfer_number": getattr(call_context, "transfer_number", None),
#         "voice": getattr(call_context, "voice", None),
#         "language": getattr(call_context, "language", None),
#     }
#     redis_client.set(f"call_context:{call_sid}", json.dumps(context_dict))

def get_call_context(call_sid):
    data = redis_client.get(f"call_context:{call_sid}")
    if not data:
        return None
    call_data = json.loads(data)
    from services.call_context import CallContext
    call_context = CallContext()
    setattr(call_context, "system_message", call_data.get("system_message"))
    setattr(call_context, "call_sid", call_data.get("call_sid"))
    setattr(call_context, "user_context", call_data.get("user_context"))
    setattr(call_context, "transfer_number", call_data.get("transfer_number"))
    setattr(call_context, "transfer_in_progress", call_data.get("transfer_in_progress"))
    setattr(call_context, "transfer_started", call_data.get("transfer_started"))
    setattr(call_context, "transfer_completed", call_data.get("transfer_completed"))
    setattr(call_context, "ari_channel_id", call_data.get("ari_channel_id"))
    setattr(call_context, "twilio_from", call_data.get("twilio_from"))
    setattr(call_context, "twilio_to", call_data.get("twilio_to"))
    setattr(call_context, "dialed_number", call_data.get("dialed_number"))
    setattr(call_context, "voice", call_data.get("voice"))
    setattr(call_context, "language", call_data.get("language"))
    setattr(call_context, "conversation_history", call_data.get("conversation_history") or [])
    setattr(call_context, "transfer_offer_pending", call_data.get("transfer_offer_pending"))
    setattr(call_context, "transfer_offer_expires_at", call_data.get("transfer_offer_expires_at"))
    setattr(call_context, "transfer_user_confirmed", call_data.get("transfer_user_confirmed"))
    setattr(call_context, "pricing_data", call_data.get("pricing_data"))
    setattr(call_context, "agent_name", call_data.get("agent_name"))
    setattr(call_context, "email_tool", call_data.get("email_tool"))
    setattr(call_context, "email_recipient", call_data.get("email_recipient"))
    setattr(call_context, "agent_type", call_data.get("agent_type"))
    setattr(call_context, "campaign_id", call_data.get("campaign_id"))
    setattr(call_context, "model", call_data.get("model"))
    setattr(call_context, "initial_greeting_twi", call_data.get("initial_greeting_twi"))
    setattr(call_context, "human_speaks_first", call_data.get("human_speaks_first"))
    setattr(call_context, "closing_text", call_data.get("closing_text"))
    setattr(call_context, "end_call_phrases", call_data.get("end_call_phrases") or [])
    return call_context

def delete_call_context(call_sid):
    redis_client.delete(f"call_context:{call_sid}")
# Global dictionary mapping CallSid to agent configuration
call_agent_mapping = {}

@app.post("/incoming")
async def incoming_call(request: Request) -> HTMLResponse:
    """Handle incoming Twilio calls and configure call context using mock API."""
    form_data = await request.form()
    incoming_twilio_number = form_data.get("To")  # Called number
    call_sid = form_data.get("CallSid")  # Unique call identifier
    incoming_twilio_from = form_data.get("From")

    if not incoming_twilio_number or not call_sid:
        logger.error(f"Missing required parameters: To={incoming_twilio_number}, CallSid={call_sid}")
        raise HTTPException(status_code=400, detail="Missing To or CallSid parameters")

    # If this is an outbound call we've already prepared, prefer the stored context
    # over inbound routing to avoid overwriting the agent/prompt.
    existing_context = get_call_context(call_sid)
    if existing_context and getattr(existing_context, "agent_type", "") == "outbound":
        logger.debug(f"Using existing outbound call context for CallSid: {call_sid}")
        save_call_context(call_sid, existing_context)
        call_contexts[call_sid] = existing_context
        server = os.getenv("SERVER")
        if not server:
            logger.error("SERVER environment variable not set")
            raise HTTPException(status_code=500, detail="SERVER environment variable not set")
        recording_callback = f"https://{server}/incoming/recording-status"
        _start_call_recording(call_sid, recording_callback)
        response = VoiceResponse()
        save_call_context(call_sid, existing_context)
        connect = Connect()
        connect.stream(url=f"wss://{server}/connection")
        response.append(connect)
        logger.debug(f"Returning TwiML for CallSid {call_sid} with WebSocket URL: wss://{server}/connection")
        return HTMLResponse(content=str(response), status_code=200)

    inbound_agents = load_agent_configs()
    if not inbound_agents:
        logger.error("No agent configurations available; falling back to defaults.")
        inbound_agents = [_default_agent()]

    # Select the agent configuration that matches the incoming Twilio number
    incoming_digits = strip_non_digits(incoming_twilio_number)
    matching_agents = []
    for agent in inbound_agents:
        agent_from = str(agent.get("from_number") or "")
        if agent_from == incoming_twilio_number:
            matching_agents.append(agent)
            continue
        if incoming_digits and strip_non_digits(agent_from) == incoming_digits:
            matching_agents.append(agent)
    matching_agent = None
    if matching_agents:
        preferred_id = os.getenv("PREFERRED_AGENT_ID", "").strip()
        if preferred_id:
            matching_agent = next(
                (agent for agent in matching_agents if str(agent.get("id")) == preferred_id),
                None
            )
        if not matching_agent:
            matching_agent = matching_agents[0]
    if matching_agent:
        agent_conf = matching_agent
        logger.info(
            "Matched agent for {}: name={}, id={}",
            incoming_twilio_number,
            agent_conf.get("name"),
            agent_conf.get("id"),
        )
    else:
        logger.warning(f"No matching agent found for {incoming_twilio_number}; using default configuration")
        agent_conf = inbound_agents[0] if inbound_agents else {}
        logger.info(f"Default agent config: {agent_conf}")

    # Save the selected agent configuration in the global mapping
    call_agent_mapping[call_sid] = agent_conf
    logger.debug(f"Stored agent config for CallSid {call_sid}: {agent_conf}")

    # Try to load existing call context (for inbound reconnects)
    existing_context = get_call_context(call_sid)
    if existing_context:
        logger.info(f"Using existing call context for CallSid: {call_sid}")
        call_context = existing_context
        # Refresh agent settings for this call to avoid stale/torn state from prior attempts.
        call_context.system_message = agent_conf.get("prompt", "") or getattr(call_context, "system_message", "")
        call_context.transfer_number = agent_conf.get("transfer_number", "") or getattr(call_context, "transfer_number", "")
        call_context.voice = agent_conf.get("voice", "") or getattr(call_context, "voice", "")
        call_context.language = agent_conf.get("language", "en-US") or getattr(call_context, "language", "en-US")
        call_context.agent_name = agent_conf.get("name") or getattr(call_context, "agent_name", "")
        call_context.email_tool = agent_conf.get("email_tool") or getattr(call_context, "email_tool", os.getenv("EMAIL_TOOL_DEFAULT", "none"))
        call_context.email_recipient = agent_conf.get("email_recipient") or getattr(call_context, "email_recipient", os.getenv("EMAIL_RECIPIENT_DEFAULT", ""))
        call_context.agent_type = agent_conf.get("agent_type") or getattr(call_context, "agent_type", None)
        call_context.pricing_data = agent_conf.get("pricing_data") or getattr(call_context, "pricing_data", None)
        call_context.agent_id = agent_conf.get("id") or getattr(call_context, "agent_id", None)
        default_latency = (os.getenv("DEFAULT_LATENCY_MODE", "turbo") or "").strip() or None
        call_context.latency_mode = (
            agent_conf.get("latency_mode")
            or getattr(call_context, "latency_mode", None)
            or default_latency
        )
        save_call_context(call_sid, call_context)
    else:
        # Create new call context for inbound call
        call_context = CallContext()
        call_context.call_sid = call_sid
        call_context.twilio_from = incoming_twilio_from
        call_context.twilio_to = incoming_twilio_number
        if incoming_twilio_number:
            call_context.dialed_number = strip_non_digits(incoming_twilio_number)
        call_context.system_message = agent_conf.get("prompt", "")
        call_context.transfer_number = agent_conf.get("transfer_number", "")
        call_context.voice = agent_conf.get("voice", os.getenv("ELEVENLABS_VOICE_ID", ""))
        call_context.language = agent_conf.get("language", "en-US")
        call_context.agent_name = agent_conf.get("name")
        call_context.email_tool = agent_conf.get("email_tool") or os.getenv("EMAIL_TOOL_DEFAULT", "none")
        call_context.email_recipient = agent_conf.get("email_recipient") or os.getenv("EMAIL_RECIPIENT_DEFAULT", "")
        default_latency = (os.getenv("DEFAULT_LATENCY_MODE", "turbo") or "").strip() or None
        call_context.latency_mode = agent_conf.get("latency_mode") or default_latency

    if agent_conf:
        call_context.agent_type = agent_conf.get("agent_type")
        call_context.pricing_data = agent_conf.get("pricing_data")
        call_context.agent_id = agent_conf.get("id")
        if not getattr(call_context, "latency_mode", None):
            call_context.latency_mode = agent_conf.get("latency_mode") or (os.getenv("DEFAULT_LATENCY_MODE", "turbo") or "").strip() or None
        call_context.human_speaks_first = agent_conf.get("human_speaks_first", False)
        agent_model = str(agent_conf.get("model") or "").strip().lower()
        if agent_model:
            call_context.model = agent_model
        if not getattr(call_context, "conversation_history", None):
            call_context.conversation_history = []
        save_call_context(call_sid, call_context)
        logger.info(f"Created new call context for CallSid: {call_sid}")

    call_contexts[call_sid] = call_context

    # Return TwiML response to instruct Twilio how to handle the call
    server = os.getenv("SERVER")
    if not server:
        logger.error("SERVER environment variable not set")
        raise HTTPException(status_code=500, detail="SERVER environment variable not set")

    recording_callback = f"https://{server}/incoming/recording-status"
    _start_call_recording(call_sid, recording_callback)

    response = VoiceResponse()
    save_call_context(call_sid, call_context)
    connect = Connect()
    connect.stream(url=f"wss://{server}/connection")
    response.append(connect)
    logger.debug(f"Returning TwiML for CallSid {call_sid} with WebSocket URL: wss://{server}/connection")
    return HTMLResponse(content=str(response), status_code=200)


@app.post("/ari/channel")
async def register_ari_channel(payload: AriChannelPayload, request: Request):
    """Register ARI channel id for a call from external call-control (e.g., Asterisk dialplan)."""
    token = os.getenv("ARI_WEBHOOK_TOKEN", "")
    if token:
        provided = request.headers.get("x-ari-token", "")
        if provided != token:
            raise HTTPException(status_code=401, detail="Unauthorized")

    call_sid = payload.call_sid.strip()
    ari_channel_id = payload.ari_channel_id.strip()
    if not call_sid or not ari_channel_id:
        raise HTTPException(status_code=422, detail="call_sid and ari_channel_id required")

    call_context = get_call_context(call_sid)
    if not call_context:
        call_context = CallContext()
        call_context.call_sid = call_sid

    call_context.ari_channel_id = ari_channel_id
    save_call_context(call_sid, call_context)
    call_contexts[call_sid] = call_context
    for key, ctx in active_stream_contexts.items():
        if key.startswith(f"{call_sid}_"):
            ctx.ari_channel_id = ari_channel_id

    logger.info(
        "Registered ARI channel id (CallSid={}, ChannelId={})",
        call_sid,
        ari_channel_id,
    )
    return {"success": True}



@app.get("/call_recording/{call_sid}")
async def get_call_recording(call_sid: str):
    """Get the recording URL and stream it to the frontend."""
    client = get_twilio_client()
    recordings = client.calls(call_sid).recordings.list()
    if recordings:
        recording = recordings[0]
        recording_url = f"https://api.twilio.com{recording.uri.replace('.json', '.mp3')}"
        
        # Fetch the recording from Twilio with authentication
        account_sid = os.getenv("TWILIO_ACCOUNT_SID")
        auth_token = os.getenv("TWILIO_AUTH_TOKEN")
        response = requests.get(recording_url, auth=(account_sid, auth_token), stream=True)
        
        if response.status_code == 200:
            # Stream the audio file as a response
            return StreamingResponse(
                response.raw,
                media_type="audio/mpeg",
                headers={"Content-Disposition": "inline; filename=recording.mp3"}
            )
        else:
            raise HTTPException(status_code=response.status_code, detail="Failed to fetch recording")
    raise HTTPException(status_code=404, detail="Recording not found")


def get_twilio_client():
    return Client(os.getenv("TWILIO_ACCOUNT_SID"), os.getenv("TWILIO_AUTH_TOKEN"))

def _select_agent_config(
    *,
    agent_name: Optional[str] = None,
    from_number: Optional[str] = None,
    agent_id: Optional[str] = None,
) -> Dict[str, Any]:
    agents = load_agent_configs()
    if not agents:
        return _default_agent()
    def _norm(value: Optional[str]) -> str:
        return str(value or "").strip().lower()
    def _digits(value: Optional[str]) -> str:
        return strip_non_digits(str(value or ""))
    if agent_id:
        wanted_id = str(agent_id).strip()
        for agent in agents:
            if str(agent.get("id") or "").strip() == wanted_id:
                return agent
    if agent_name:
        wanted = _norm(agent_name)
        for agent in agents:
            if _norm(agent.get("name")) == wanted:
                return agent
    if from_number:
        wanted_from = str(from_number).strip()
        wanted_digits = _digits(wanted_from)
        for agent in agents:
            agent_from = str(agent.get("from_number") or "").strip()
            if agent_from == wanted_from:
                return agent
            if wanted_digits and _digits(agent_from) == wanted_digits:
                return agent
    return agents[0]

# API route to initiate a call via UI
from typing import List, Dict
from fastapi import Body

@app.post("/start_call")
async def start_call(
    to_numbers: List[str] = Body(...),
    system_message: Optional[str] = Body(None),
    # initial_message removed (agent should follow prompt only)
    transfer_number: str = Body(...),
    twilio_number: str = Body(...),          # required only for PSTN
    voice: str = Body(...),
    language: str = Body(...),
    email_tool: Optional[str] = Body(None),
    email_recipient: Optional[str] = Body(None),
    agent_id: Optional[str] = Body(None),
    agent_name: Optional[str] = Body(None),
    agent_type: Optional[str] = Body(None),
    model: Optional[str] = Body(None),
    human_speaks_first: Optional[bool] = Body(None),
):
    """Initiate one or many outbound calls."""
    # -------------------------- validation --------------------------
    # initial_message removed
    if not to_numbers or not transfer_number or not voice or not language:
        raise HTTPException(
            status_code=400,
            detail="Missing required fields (to_numbers, transfer_number, voice, language)"
        )

    SUPPORTED_LANGS = {
        "bg","ca","zh","zh-TW","zh-HK","cs","da","nl","en-US","en-GB","hi",
        "fr","de","ja","ko","es","sv","pt","it","ru","tr","vi","th","pl"
    }
    if language not in SUPPORTED_LANGS:
        raise HTTPException(status_code=400, detail=f"Unsupported language: {language}")

    # SIP detection
    sip_enabled = all([
        os.getenv("TWILIO_SIP_DOMAIN"),
        os.getenv("TWILIO_SIP_USERNAME"),
        os.getenv("ASTERISK_SIP_HOST")
    ])
    if not sip_enabled and not twilio_number:
        raise HTTPException(
            status_code=400,
            detail="twilio_number required when SIP trunking is not configured"
        )

    # -------------------------- URLs --------------------------
    server = os.getenv("SERVER")
    if not server:
        raise HTTPException(status_code=500, detail="SERVER env var missing")
    service_url     = f"https://{server}/incoming"
    status_callback = f"https://{server}/end_call_status"
    recording_callback = f"https://{server}/incoming/recording-status"

    client = get_twilio_client()
    results = []
    agent_conf = _select_agent_config(agent_id=agent_id, agent_name=agent_name, from_number=twilio_number)
    prefer_agent_defaults = bool(agent_id or agent_name)
    agent_prompt = system_message or agent_conf.get("prompt") or read_global_prompt()
    agent_first_sentence = ""
    agent_type_value = agent_type or agent_conf.get("agent_type") or "outbound"
    if prefer_agent_defaults:
        voice = agent_conf.get("voice") or voice
        language = agent_conf.get("language") or language
        transfer_number = agent_conf.get("transfer_number") or transfer_number
    email_tool_value = email_tool or agent_conf.get("email_tool") or os.getenv("EMAIL_TOOL_DEFAULT", "none")
    email_recipient_value = email_recipient or agent_conf.get("email_recipient") or os.getenv("EMAIL_RECIPIENT_DEFAULT", "")
    agent_model = str((model or agent_conf.get("model") or "")).strip().lower()
    # Prewarm pipeline components to minimize first-turn latency.
    try:
        prewarm_stt = os.getenv("PREWARM_STT", "true").lower() == "true"
        prewarm_tts = os.getenv("PREWARM_TTS", "true").lower() == "true"
        prewarm_llm = os.getenv("PREWARM_LLM", "true").lower() == "true"
        if prewarm_stt:
            asyncio.create_task(_prewarm_elevenlabs_stt())
        if prewarm_tts:
            asyncio.create_task(_prewarm_elevenlabs_tts(voice))
        if prewarm_llm:
            asyncio.create_task(_prewarm_openai_llm(agent_model or os.getenv("OPENAI_MODEL", "")))
    except Exception:
        pass

    # -------------------------- call loop --------------------------
    for dest in to_numbers:
        try:
            call_sid: Optional[str] = None

            # ------------------ SIP mode ------------------
            if sip_enabled:
                sip_to   = f"sip:{dest}@{os.getenv('ASTERISK_SIP_HOST')}"
                sip_from = os.getenv('TWILIO_SIP_USERNAME')   # USERNAME ONLY

                call = await _create_twilio_call(
                    client,
                    {
                        "to": sip_to,
                        "from_": sip_from,
                        "url": service_url,
                        "status_callback": status_callback,
                        "status_callback_method": "POST",
                        "record": True,
                        "recording_status_callback": recording_callback,
                        "recording_status_callback_method": "POST",
                        "recording_status_callback_event": ["completed"],
                        "timeout": 55,
                    },
                )
                call_sid = call.sid
                logger.info(f"SIP call to {sip_to} (SID {call_sid})")

            # ------------------ PSTN mode ------------------
            else:
                call = await _create_twilio_call(
                    client,
                    {
                        "to": dest,
                        "from_": twilio_number,
                        "url": service_url,
                        "status_callback": status_callback,
                        "status_callback_method": "POST",
                        "record": True,
                        "recording_status_callback": recording_callback,
                        "recording_status_callback_method": "POST",
                        "recording_status_callback_event": ["completed"],
                        "timeout": 55,
                    },
                )
                call_sid = call.sid
                logger.info(f"PSTN call to {dest} (SID {call_sid})")

            # ------------------ context & TTS ------------------
            ctx = CallContext()
            ctx.system_message  = agent_prompt
            # no initial_message
            ctx.pricing_data    = agent_conf.get("pricing_data")
            ctx.call_sid        = call_sid
            ctx.transfer_number = transfer_number
            ctx.voice           = voice
            ctx.language        = language
            ctx.agent_name      = agent_name or agent_conf.get("name")
            ctx.email_tool      = email_tool_value
            ctx.email_recipient = email_recipient_value
            ctx.agent_type      = agent_type_value
            default_latency = (os.getenv("DEFAULT_LATENCY_MODE", "turbo") or "").strip() or None
            ctx.latency_mode    = agent_conf.get("latency_mode") or default_latency
            ctx.human_speaks_first = bool(human_speaks_first or agent_conf.get("human_speaks_first"))
            ctx.twilio_from     = twilio_number
            ctx.twilio_to       = dest
            ctx.force_rule_based = (agent_type_value == "outbound")
            if not ctx.force_rule_based and agent_prompt:
                if "rules you must follow" in normalize_whitespace(agent_prompt).lower():
                    ctx.force_rule_based = True
            if ctx.pricing_data or "bhk" in (agent_prompt or "").lower():
                ctx.force_pricing_guardrails = True
            if agent_model:
                ctx.model = agent_model
            if dest:
                ctx.dialed_number = strip_non_digits(dest)
            ctx.conversation_history = []
            save_call_context(call_sid, ctx)
            call_contexts[call_sid] = ctx
            call_agent_mapping[call_sid] = agent_conf

            results.append({"call_sid": call_sid, "to_number": dest})

        except Exception as exc:
            logger.error(f"Call to {dest} failed: {exc}", exc_info=True)
            results.append({"error": f"Failed to call {dest}: {str(exc)}"})

    return {"results": results}






from fastapi import HTTPException

# API route to get the status of a call
@app.get("/call_status/{call_sid}")
async def get_call_status(call_sid: str):
    """Get the status of a call."""
    try:
        client = get_twilio_client()
        call = client.calls(call_sid).fetch()
        return {"status": call.status}
    except Exception as e:
        logger.error(f"Error fetching call status: {str(e)}")
        return {"error": f"Failed to fetch call status: {str(e)}"}


def _resolve_campaign_contact_result(call_status: str, answered: bool, transferred: bool) -> str:
    normalized = str(call_status or "").strip().lower()
    if transferred:
        return "transferred"
    if normalized in {"busy", "failed", "no-answer", "canceled"}:
        return "failed"
    if normalized == "completed" and answered:
        return "answered"
    if normalized in {"queued", "ringing", "in-progress"}:
        return "active"
    if normalized == "completed":
        return "completed"
    return normalized or "unknown"


def _coerce_bool_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    raw = str(value or "").strip().lower()
    return raw in {"1", "true", "yes", "y"}


def _effective_contact_result(item: Dict[str, Any]) -> str:
    explicit = str(item.get("result") or "").strip()
    if explicit:
        return explicit
    return _resolve_campaign_contact_result(
        str(item.get("status") or ""),
        _coerce_bool_flag(item.get("answered")),
        _coerce_bool_flag(item.get("transferred")),
    )


def _fallback_campaign_contacts_from_context(campaign_id: str) -> List[Dict[str, Any]]:
    rows: Dict[str, Dict[str, Any]] = {}
    try:
        for raw_key in redis_client.scan_iter(match="call_context:*"):
            raw_payload = redis_client.get(raw_key)
            if not raw_payload:
                continue
            try:
                payload = json.loads(raw_payload.decode("utf-8"))
            except Exception:
                continue
            if str(payload.get("campaign_id") or "") != str(campaign_id):
                continue
            call_sid = str(payload.get("call_sid") or "").strip()
            phone = str(payload.get("twilio_to") or payload.get("dialed_number") or "").strip()
            dedupe_key = call_sid or phone
            if not dedupe_key:
                continue
            item = {
                "name": str(payload.get("lead_name") or "").strip(),
                "phone_number": phone,
                "call_sid": call_sid,
                "status": str(payload.get("final_status") or "").strip(),
                "result": "",
                "answered": 0,
                "transferred": 1 if _coerce_bool_flag(payload.get("transfer_completed")) else 0,
                "duration_sec": 0,
                "initiated_at": str(payload.get("created_at") or ""),
                "completed_at": str(payload.get("completed_at") or ""),
            }
            item["result"] = _effective_contact_result(item)
            rows[dedupe_key] = item
    except Exception:
        return []
    return list(rows.values())


# API route to end a call
@app.post("/end_call_status")
async def end_call_status(
    CallSid: str = Form(...),
    CallStatus: str = Form(...),
    CallDuration: Optional[str] = Form(None),
):
    logger.info(f"Received status callback for CallSid: {CallSid} with status: {CallStatus}")
    call_context = call_contexts.get(CallSid) or get_call_context(CallSid)
    if call_context:
        call_context.final_status = CallStatus
        if CallStatus in {"completed", "busy", "failed", "no-answer", "canceled"}:
            call_context.completed_at = datetime.utcnow().isoformat()
        save_call_context(CallSid, call_context)

    final_statuses = {"completed", "busy", "failed", "no-answer", "canceled"}
    declined_statuses = {"busy", "failed", "no-answer", "canceled"}
    if call_context and getattr(call_context, "campaign_id", None) and CallStatus in final_statuses:
        campaign_id = call_context.campaign_id
        duration_sec = 0
        answered = False
        if CallStatus == "completed":
            if CallDuration is not None:
                try:
                    duration_sec = int(str(CallDuration).strip() or "0")
                except (TypeError, ValueError):
                    duration_sec = 0
            if duration_sec <= 0:
                try:
                    call = await asyncio.to_thread(get_twilio_client().calls(CallSid).fetch)
                    duration_sec = int(getattr(call, "duration", 0) or 0)
                except Exception:
                    duration_sec = 0
            answered = duration_sec > 0
        transfer_completed = bool(getattr(call_context, "transfer_completed", False))
        if mark_call_finalized(campaign_id, CallSid):
            increment_campaign(campaign_id, "completed", 1)
            if answered:
                increment_campaign(campaign_id, "answered", 1)
            if CallStatus in declined_statuses:
                increment_campaign(campaign_id, "declined", 1)
            else:
                increment_campaign(campaign_id, "success", 1)
            snapshot = get_campaign(campaign_id)
            if snapshot:
                total_targets = int(snapshot.get("total_targets", 0))
                completed = int(snapshot.get("completed", 0))
                failed_initiate = int(snapshot.get("failed_initiate", 0))
                if total_targets and (completed + failed_initiate) >= total_targets:
                    set_campaign_fields(
                        campaign_id,
                        {
                            "status": "completed",
                            "completed_at": datetime.utcnow().isoformat(),
                        },
                    )
        update_campaign_contact_by_call_sid(
            campaign_id,
            CallSid,
            {
                "status": CallStatus,
                "duration_sec": duration_sec,
                "answered": int(answered),
                "transferred": int(transfer_completed),
                "result": _resolve_campaign_contact_result(CallStatus, answered, transfer_completed),
                "completed_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
            },
        )

    if CallStatus == "completed":
        transcript_file_path = os.path.join(LOG_DIR, f"{CallSid}.txt")
        if os.path.exists(transcript_file_path):
            with open(transcript_file_path, "r") as file:
                transcript_content = file.read()
            asyncio.create_task(maybe_send_lead_email(CallSid, transcript_content))
            return {"status": "success"}
        logger.warning(f"No transcript found for CallSid {CallSid}")
        return {"status": "completed", "message": "No transcript found"}
    return {"status": "ignored", "message": "Call status not relevant"}



# API call to get the transcript for a specific call
@app.get("/transcript/{call_sid}")
async def get_transcript(call_sid: str):
    """Get the entire transcript for a specific call."""
    call_context = call_contexts.get(call_sid)

    if not call_context:
        logger.info(f"[GET] Call not found for call SID: {call_sid}")
        return {"error": "Call not found"}

    return {"transcript": call_context.user_context}


from fastapi import Form

@app.post("/incoming/recording-status")
async def recording_status(RecordingSid: str = Form(...), CallSid: str = Form(...)):
    logger.info(f"RecordingSid: {RecordingSid}, CallSid: {CallSid}")

    # Request transcription for the recording.  Unfortunately the
    # version of the Twilio helper library we have installed does not
    # expose a ``create`` method on the Recording/Transcription list
    # objects.  In the logs we were seeing:
    #
    #     TranscriptionList has no create / 'TranscriptionList' object
    #     has no attribute 'create'
    #
    # which is exactly what happens when the SDK doesn't support the
    # operation.  Rather than blindly raising or upgrading the library
    # we fall back to doing a straight HTTP POST to the Twilio REST
    # API.  That endpoint is well documented and stable, so this keeps
    # the application working regardless of helper library version.

    client = get_twilio_client()
    try:
        # first attempt using the helper library; this is fast when it
        # exists and keeps behaviour familiar for newer versions.
        transcription = None
        try:
            transcriptions = client.recordings(RecordingSid).transcriptions
            if hasattr(transcriptions, "create"):
                transcription = transcriptions.create(language="en-US")
            else:
                # helper library is old and doesn't support create
                raise AttributeError("TranscriptionList has no create")
        except Exception as first_exc:
            # library call failed; fall back to manual REST request
            try:
                account_sid = os.getenv("TWILIO_ACCOUNT_SID")
                auth_token = os.getenv("TWILIO_AUTH_TOKEN")
                url = (
                    f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}"
                    f"/Recordings/{RecordingSid}/Transcriptions.json"
                )
                resp = requests.post(
                    url,
                    auth=(account_sid, auth_token),
                    data={"Language": "en-US"},
                    timeout=10,
                )
                if resp.status_code not in (200, 201):
                    raise Exception(f"HTTP {resp.status_code}: {resp.text}")
                transcription = resp.json()
            except Exception as second_exc:
                # both library and manual calls failed; log and give up
                logger.warning(
                    "Transcription request skipped for RecordingSid {}: {} / {}",
                    RecordingSid,
                    first_exc,
                    second_exc,
                )
                transcription = None
        if transcription is not None:
            logger.info(f"Transcription requested for RecordingSid: {RecordingSid}")
    except Exception as e:  # pragma: no cover - defensive
        # catch anything else so the webhook always returns 200
        logger.warning(f"Error requesting transcription: {str(e)}")
    
    return {"status": "received"}


@app.get("/check_transcription/{recording_sid}")
async def check_transcription(recording_sid: str):
    client = get_twilio_client()
    try:
        transcription = client.recordings(recording_sid).transcriptions.list()
    except Exception as e:
        logger.warning(
            "Error fetching transcription list for {}: {}", recording_sid, e
        )
        return {"error": "failed to query transcription"}

    if transcription:
        return {"status": transcription[0].status, "transcription": transcription[0].transcription_text}
    else:
        return {"error": "No transcription available"}


# API route to get all call transcripts
@app.get("/all_transcripts")
async def get_all_transcripts():
    """Get a list of all current call transcripts."""
    try:
        transcript_list = []
        for call_sid, context in call_contexts.items():
            transcript_list.append({
                "call_sid": call_sid,
                "transcript": context.user_context,
            })
        return {"transcripts": transcript_list}
    except Exception as e:
        logger.error(f"Error fetching all transcripts: {str(e)}")
        return {"error": f"Failed to fetch all transcripts: {str(e)}"}


TRANSFER_NUMBERS_FILE = os.getenv(
    "TRANSFER_NUMBERS_FILE",
    str(BASE_DIR / "transfernumbers.json"),
)

# Function to read transfer numbers from file
def read_transfer_numbers():
    try:
        if os.path.exists(TRANSFER_NUMBERS_FILE):
            with open(TRANSFER_NUMBERS_FILE, "r") as file:
                content = file.read().strip()  # Read and strip any extra whitespace
                if content:  # Check if the file is not empty
                    return json.loads(content)
                else:
                    return []  # Return an empty list if the file is empty
        return []  # Return an empty list if the file does not exist
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {TRANSFER_NUMBERS_FILE}: {str(e)}")
        return []  # Return an empty list if JSON decoding fails

# Function to save transfer numbers to file
def write_transfer_numbers(data):
    try:
        with open(TRANSFER_NUMBERS_FILE, "w") as file:
            json.dump(data, file, indent=4)
    except Exception as e:
        logger.error(f"Error writing to {TRANSFER_NUMBERS_FILE}: {str(e)}")


@app.post("/save_transfer_number")
async def save_transfer_number(request: Request):
    try:
        # Parse the request body as JSON
        body = await request.json()
        logger.info(f"Received request body: {body}")

        name = body.get('name')
        phone_number = body.get('phone_number')

        if not name or not phone_number:
            raise HTTPException(status_code=400, detail="Name and phone number are required")

        transfer_numbers = read_transfer_numbers()

        # Check if the phone number already exists
        for number in transfer_numbers:
            if number["phone_number"] == phone_number:
                raise HTTPException(status_code=400, detail="Phone number already exists")

        # Add new transfer number
        transfer_numbers.append({"name": name, "phone_number": phone_number})
        write_transfer_numbers(transfer_numbers)

        return {"success": True}

    except json.JSONDecodeError as e:
        logger.error(f"JSONDecodeError: {str(e)}")
        raise HTTPException(status_code=400, detail="Invalid JSON data")
    except Exception as e:
        logger.error(f"Error saving transfer number: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

    
# API to get saved transfer numbers
@app.get("/get_transfer_numbers")
async def get_transfer_numbers():
    transfer_numbers = read_transfer_numbers()
    return {"numbers": transfer_numbers}

# API to delete a transfer number
@app.delete("/delete_transfer_number/{phone_number}")
async def delete_transfer_number(phone_number: str):
    transfer_numbers = read_transfer_numbers()
    updated_numbers = [num for num in transfer_numbers if num["phone_number"] != phone_number]

    if len(transfer_numbers) == len(updated_numbers):
        raise HTTPException(status_code=404, detail="Phone number not found")

    write_transfer_numbers(updated_numbers)
    return {"success": True}


# Function to get the most recent CSV file in the uploads folder
def get_latest_csv_file(upload_folder):
    try:
        # List all files in the upload folder
        files = [f for f in os.listdir(upload_folder) if f.endswith('.csv')]
        
        if not files:
            raise FileNotFoundError("No CSV files found in the uploads folder.")
        
        # Select the most recent file based on modification time
        latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(upload_folder, f)))
        return os.path.join(upload_folder, latest_file)
    except Exception as e:
        raise FileNotFoundError(f"Error finding CSV file: {str(e)}")

# Function to read contacts from the latest CSV file
def read_csv_file(file_path):
    contacts = []
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header if it exists
        for row in reader:
            if len(row) >= 2:  # Ensure there's a name and phone number
                contacts.append({"name": row[0], "phone_number": row[1]})
    return contacts

# Function to write contacts back to the CSV after removing processed ones
def write_csv_file(file_path, contacts):
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Name', 'Phone Number'])  # Assuming the CSV has a header
        for contact in contacts:
            writer.writerow([contact['name'], contact['phone_number']])
class _UserTranscriptLogger(FrameProcessor):
    def __init__(self, call_context: CallContext, on_user_text=None, debounce_sec: float = 0.6):
        super().__init__()
        self._call_context = call_context
        self._on_user_text = on_user_text
        self._debounce_sec = debounce_sec
        self._barge_min_words = int(os.getenv("BARGE_IN_MIN_WORDS", "1"))
        self._barge_cooldown = float(os.getenv("BARGE_IN_COOLDOWN_SEC", "1.0"))
        self._barge_ignore_after_tts_sec = float(os.getenv("BARGE_IN_IGNORE_AFTER_TTS_SEC", "0.35"))
        self._barge_min_words_speaking = int(os.getenv("BARGE_IN_MIN_WORDS_WHILE_SPEAKING", "2"))
        stop_words_raw = os.getenv(
            "BARGE_IN_ALLOW_STOP_WORDS",
            "stop,wait,hold on,one second,just a second,excuse me,are you there,you there,can you hear me",
        )
        self._barge_stop_words = {w.strip().lower() for w in stop_words_raw.split(",") if w.strip()}
        filler_raw = os.getenv(
            "USER_FILLER_IGNORE_WORDS",
            "uh huh,uh-huh,uhuh,uhhuh,mhm,mm hmm,mm-hmm,mmhmm,aha,ah ha,aha ha,um,uh,hm,hmm,mm,mmm,ok,okay,oh,ah,ha",
        )
        self._filler_words = {w.strip().lower() for w in filler_raw.split(",") if w.strip()}
        self._filler_ignore_after_sec = float(os.getenv("FILLER_IGNORE_AFTER_ASSISTANT_SEC", "0.8"))
        self._dedupe_window = float(os.getenv("USER_TRANSCRIPT_DEDUPE_WINDOW_SEC", "8.0"))
        self._last_normalized = ""
        self._last_ts = 0.0
        self._buffer: List[str] = []
        self._flush_task: Optional[asyncio.Task] = None

    @staticmethod
    def _collapse_repetitions(text: str) -> str:
        normalized = _normalize_for_match(text)
        tokens = [t for t in normalized.split() if t]
        if len(tokens) < 4:
            return normalized
        out: List[str] = []
        i = 0
        max_k = 12
        while i < len(tokens):
            matched = False
            limit = min(max_k, (len(tokens) - i) // 2)
            for k in range(1, limit + 1):
                if tokens[i : i + k] == tokens[i + k : i + 2 * k]:
                    out.extend(tokens[i : i + k])
                    i += 2 * k
                    matched = True
                    break
            if not matched:
                out.append(tokens[i])
                i += 1
        collapsed = " ".join(out).strip()
        tokens = [t for t in collapsed.split() if t]
        if len(tokens) >= 8:
            head = tokens[:2]
            last_idx = -1
            for j in range(len(tokens) - 1):
                if tokens[j : j + 2] == head:
                    last_idx = j
            if last_idx > 0 and (len(tokens) - last_idx) >= 4:
                return " ".join(tokens[last_idx:]).strip()
        return collapsed

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, TranscriptionFrame):
            text = (frame.text or "").strip()
            if text:
                self._buffer.append(text)
                self._schedule_flush()
        await self.push_frame(frame, direction)

    def _schedule_flush(self):
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
        self._flush_task = asyncio.create_task(self._flush_after_delay())

    async def _flush_after_delay(self):
        try:
            await asyncio.sleep(max(0.05, self._debounce_sec))
        except asyncio.CancelledError:
            return
        if not self._buffer:
            return
        text = " ".join(self._buffer).strip()
        self._buffer.clear()
        text = self._collapse_repetitions(text)
        normalized = _normalize_for_match(text)
        now = time.monotonic()
        if normalized and normalized == self._last_normalized and (now - self._last_ts) < self._dedupe_window:
            return
        if self._call_context:
            if normalized in self._filler_words and len(text.split()) <= 2:
                # Always ignore short fillers even when not speaking.
                if getattr(self._call_context, "tts_speaking", False):
                    return
                last_assistant_ts = getattr(self._call_context, "last_assistant_utterance_ts", 0) or 0
                if last_assistant_ts and (now - last_assistant_ts) < self._filler_ignore_after_sec:
                    return
                return
            if not getattr(self._call_context, "last_assistant_utterance_ts", 0):
                if normalized in {"ok", "okay", "hmm", "hm", "oh"}:
                    return
            else:
                if getattr(self._call_context, "tts_speaking", False):
                    if self._barge_min_words_speaking > 0:
                        words = len(text.split())
                        if words < self._barge_min_words_speaking:
                            if normalized not in self._barge_stop_words:
                                return
                if self._barge_min_words > 0:
                    words = len(text.split())
                    if words < self._barge_min_words:
                        last_ts = getattr(self._call_context, "last_assistant_utterance_ts", 0) or 0
                        if last_ts and (time.monotonic() - last_ts) <= self._barge_cooldown:
                            return
                tts_start_ts = getattr(self._call_context, "last_tts_first_audio_ts", 0) or 0
                if tts_start_ts and (time.monotonic() - tts_start_ts) < self._barge_ignore_after_tts_sec:
                    return
        if text and self._call_context:
            self._last_normalized = normalized
            self._last_ts = now
            self._call_context.last_user_utterance_ts = time.monotonic()
            self._call_context.last_user_final_ts = time.monotonic()
            self._call_context.last_user_text = text
            if not getattr(self._call_context, "conversation_history", None):
                self._call_context.conversation_history = []
            self._call_context.conversation_history.append({"speaker": "user", "message": text})
            if not getattr(self._call_context, "user_context", None):
                self._call_context.user_context = []
            self._call_context.user_context.append({"role": "user", "content": text})
            save_call_context(self._call_context.call_sid, self._call_context)
            save_to_log(self._call_context.call_sid, "user", text)
            if self._on_user_text:
                asyncio.create_task(self._on_user_text(text))


class _TranscriptionDeduper(FrameProcessor):
    def __init__(self, window_sec: float = 1.5):
        super().__init__()
        self._window_sec = window_sec
        self._last_text = ""
        self._last_ts = 0.0

    @staticmethod
    def _normalize(text: str) -> str:
        cleaned = []
        for ch in text.lower():
            if ch.isalnum() or ch.isspace():
                cleaned.append(ch)
            else:
                cleaned.append(" ")
        return " ".join("".join(cleaned).split())

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, TranscriptionFrame):
            text = (frame.text or "").strip()
            if text:
                now = time.monotonic()
                normalized = self._normalize(text)
                if normalized and self._last_text and (now - self._last_ts) < self._window_sec:
                    if normalized == self._last_text:
                        return
                    if normalized in self._last_text or self._last_text in normalized:
                        return
                    try:
                        ratio = SequenceMatcher(None, normalized, self._last_text).ratio()
                        if ratio >= 0.88:
                            return
                    except Exception:
                        pass
                self._last_text = normalized
                self._last_ts = now
        await self.push_frame(frame, direction)


class _TranscriptNoiseFilter(FrameProcessor):
    def __init__(
        self,
        min_chars: int = 2,
        min_alpha: int = 2,
        ignore_tokens: Optional[Set[str]] = None,
    ):
        super().__init__()
        self._min_chars = max(1, min_chars)
        self._min_alpha = max(0, min_alpha)
        self._ignore_tokens = ignore_tokens or {
            "um",
            "uh",
            "er",
            "ah",
            "aha",
            "ah ha",
            "aha ha",
            "hmm",
            "hm",
            "mm",
            "mmm",
            "mhm",
            "mm hmm",
            "mm-hmm",
            "mmhmm",
            "uhh",
            "uh huh",
            "uh-huh",
            "uhuh",
            "uhhuh",
            "huh",
            "ok",
            "okay",
            "oh",
            "ha",
        }

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, (TranscriptionFrame, InterimTranscriptionFrame)):
            text = (frame.text or "").strip()
            if not text:
                return
            normalized = _normalize_for_match(text)
            if normalized in self._ignore_tokens:
                return
            alpha = sum(1 for ch in text if ch.isalpha())
            digits = sum(1 for ch in text if ch.isdigit())
            if alpha + digits < self._min_chars:
                return
            if digits == 0 and alpha < self._min_alpha:
                return
        await self.push_frame(frame, direction)


class _AssistantEchoFilter(FrameProcessor):
    """Suppress likely assistant-audio echo captured by STT during playback."""

    def __init__(self, call_context: CallContext):
        super().__init__()
        self._call_context = call_context
        self._active_window_sec = float(os.getenv("ASSISTANT_ECHO_ACTIVE_WINDOW_SEC", "1.0"))
        self._min_words = int(os.getenv("ASSISTANT_ECHO_MIN_WORDS", "2"))
        self._similarity = float(os.getenv("ASSISTANT_ECHO_SIMILARITY", "0.86"))
        allow_raw = os.getenv(
            "ASSISTANT_ECHO_ALLOW_PHRASES",
            "stop,wait,hold on,excuse me,one second,are you there",
        )
        self._allow_phrases = {w.strip().lower() for w in allow_raw.split(",") if w.strip()}

    def _assistant_active(self, now: float) -> bool:
        if not self._call_context:
            return False
        if getattr(self._call_context, "tts_speaking", False):
            return True
        last_stop = getattr(self._call_context, "last_tts_stop_ts", 0) or 0
        last_start = getattr(self._call_context, "last_tts_start_ts", 0) or 0
        ref = max(last_stop, last_start)
        return bool(ref and (now - ref) <= self._active_window_sec)

    def _is_echo(self, text: str) -> bool:
        normalized = _normalize_for_match(text)
        if not normalized:
            return False
        if normalized in self._allow_phrases:
            return False
        words = [w for w in normalized.split() if w]
        if len(words) < max(1, self._min_words):
            return False
        assistant_text = ""
        if self._call_context:
            assistant_text = (
                getattr(self._call_context, "last_assistant_message", "") or ""
            )
        assistant_norm = _normalize_for_match(assistant_text)
        if not assistant_norm:
            return False
        if normalized in assistant_norm or assistant_norm.startswith(normalized):
            return True
        assistant_words = assistant_norm.split()
        if not assistant_words:
            return False
        prefix_len = min(len(assistant_words), len(words) + 2)
        prefix = " ".join(assistant_words[:prefix_len]).strip()
        if not prefix:
            return False
        try:
            ratio = SequenceMatcher(None, normalized, prefix).ratio()
            return ratio >= self._similarity
        except Exception:
            return False

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, (InterimTranscriptionFrame, TranscriptionFrame)):
            text = (frame.text or "").strip()
            if text and self._assistant_active(time.monotonic()) and self._is_echo(text):
                logger.debug("Dropping likely assistant echo transcript: {}", text)
                return
        await self.push_frame(frame, direction)


class _UserStartGate(FrameProcessor):
    def __init__(self, *_args, **_kwargs):
        super().__init__()

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(
            frame,
            (
                UserStartedSpeakingFrame,
                UserStoppedSpeakingFrame,
                EmulateUserStartedSpeakingFrame,
                EmulateUserStoppedSpeakingFrame,
                VADUserStartedSpeakingFrame,
                VADUserStoppedSpeakingFrame,
            ),
        ):
            # Let TurnDetector emit its own start/stop once speech is validated.
            return
        await self.push_frame(frame, direction)


class _TurnDetector(FrameProcessor):
    def __init__(self, call_context: CallContext):
        super().__init__()
        self._call_context = call_context
        self._silence_ms = int(os.getenv("TURN_SILENCE_MS", "450"))
        self._final_silence_ms = int(os.getenv("TURN_FINAL_SILENCE_MS", "300"))
        self._min_speech_ms = int(os.getenv("TURN_MIN_SPEECH_MS", "150"))
        self._coalesce_sec = float(os.getenv("TURN_COALESCE_SEC", "3.0"))
        self._coalesce_sim = float(os.getenv("TURN_COALESCE_SIM", "0.9"))
        self._buffer: List[str] = []
        self._speaking = False
        self._vad_seen = False
        self._speech_start_ts: Optional[float] = None
        self._speech_stop_ts: Optional[float] = None
        self._turn_task: Optional[asyncio.Task] = None
        self._barge_task: Optional[asyncio.Task] = None
        self._pending_start = False
        self._started_emitted = False
        self._last_text_ts: Optional[float] = None
        self._last_norm_text: Optional[str] = None
        self._last_norm_ts: Optional[float] = None
        self._last_emitted_norm: Optional[str] = None
        self._last_emit_ts: Optional[float] = None
        self._pending_interim: Optional[str] = None
        self._pending_interim_norm: Optional[str] = None
        self._last_interim_norm: Optional[str] = None
        self._last_interim_ts: Optional[float] = None
        self._barge_max_wait_sec = float(os.getenv("BARGE_IN_MAX_WAIT_SEC", "0.6"))
        self._barge_poll_sec = float(os.getenv("BARGE_IN_POLL_SEC", "0.05"))
        self._barge_interim_max_age = float(os.getenv("BARGE_IN_INTERIM_MAX_AGE_SEC", "0.8"))
        self._barge_min_words = int(os.getenv("BARGE_IN_MIN_WORDS", "2"))
        self._barge_min_words_speaking = int(os.getenv("BARGE_IN_MIN_WORDS_WHILE_SPEAKING", "2"))
        self._barge_ignore_after_tts_sec = float(os.getenv("BARGE_IN_IGNORE_AFTER_TTS_SEC", "0.35"))
        self._greeting_ignore_window_sec = float(os.getenv("TURN_IGNORE_GREETING_WINDOW_SEC", "1.2"))
        stop_words_raw = os.getenv(
            "BARGE_IN_ALLOW_STOP_WORDS",
            "stop,wait,hold on,one second,just a second,excuse me,are you there,you there,can you hear me",
        )
        self._barge_stop_words = {w.strip().lower() for w in stop_words_raw.split(",") if w.strip()}
        greeting_raw = os.getenv(
            "TURN_GREETINGS",
            "hello,hi,hey,hiya,heya,greetings,namaste",
        )
        self._greetings = {w.strip().lower() for w in greeting_raw.split(",") if w.strip()}
        filler_raw = os.getenv(
            "TURN_FILLERS",
            "um,uh,ah,ahh,uhh,uh-huh,uh huh,mm,mm-hmm,mhm,hmm,ok,okay,oh,aha,ah ha,ha,huh,sorry,and",
        )
        self._fillers = {w.strip().lower() for w in filler_raw.split(",") if w.strip()}
        ack_raw = os.getenv(
            "TURN_ACK_WORDS",
            "uh-huh,uh huh,mm-hmm,mm hmm,mhm,okay,ok,oh,alright,all right,right,yeah,yep,yup,sorry,and",
        )
        self._ack_words = {w.strip().lower() for w in ack_raw.split(",") if w.strip()}

    def _is_filler_only(self, text: str) -> bool:
        if not text:
            return True
        words = [w for w in _normalize_for_match(text).split() if w]
        if not words:
            return True
        return all(w in self._fillers for w in words)

    def _has_meaningful_words(self, text: str) -> bool:
        words = [w for w in _normalize_for_match(text).split() if w]
        if not words:
            return False
        return any(w not in self._fillers for w in words)

    def _is_ack_only(self, text: str) -> bool:
        words = [w for w in _normalize_for_match(text).split() if w]
        if not words:
            return False
        return all(w in self._ack_words for w in words)

    def _barge_threshold_met(self, text: str) -> bool:
        normalized = _normalize_for_match(text)
        if not normalized:
            return False
        words = [w for w in normalized.split() if w]
        if not words:
            return False
        min_words = self._barge_min_words
        if self._call_context and getattr(self._call_context, "tts_speaking", False):
            min_words = self._barge_min_words_speaking
        if len(words) >= max(1, min_words):
            return True
        return normalized in self._barge_stop_words

    def _is_greeting_only(self, text: str) -> bool:
        words = [w for w in _normalize_for_match(text).split() if w]
        if not words:
            return False
        if words[0] not in self._greetings:
            return False
        return len(words) <= 3

    def _ignore_greeting(self, text: str) -> bool:
        if not self._is_greeting_only(text):
            return False
        if not self._call_context:
            return False
        if getattr(self._call_context, "tts_speaking", False):
            return False
        last_assistant_ts = getattr(self._call_context, "last_assistant_utterance_ts", 0) or 0
        if not last_assistant_ts:
            return False
        if (time.monotonic() - last_assistant_ts) > self._greeting_ignore_window_sec:
            return False
        last_q = getattr(self._call_context, "last_assistant_question", None)
        last_msg = getattr(self._call_context, "last_assistant_message", None)
        return bool(last_q or last_msg)

    def _dedupe_buffer(self) -> str:
        deduped: List[str] = []
        seen: Set[str] = set()
        for part in self._buffer:
            norm = _normalize_for_match(part)
            if not norm:
                continue
            if norm in seen:
                continue
            seen.add(norm)
            deduped.append(part)
        text = " ".join(deduped).strip()
        return self._collapse_repetitions(text)

    @staticmethod
    def _collapse_repetitions(text: str) -> str:
        tokens = [t for t in text.split() if t]
        if len(tokens) < 4:
            return text
        out: List[str] = []
        i = 0
        max_k = 12
        while i < len(tokens):
            matched = False
            limit = min(max_k, (len(tokens) - i) // 2)
            for k in range(1, limit + 1):
                if tokens[i : i + k] == tokens[i + k : i + 2 * k]:
                    out.extend(tokens[i : i + k])
                    i += 2 * k
                    matched = True
                    break
            if not matched:
                out.append(tokens[i])
                i += 1
        collapsed = " ".join(out).strip()
        tokens = [t for t in collapsed.split() if t]
        if len(tokens) >= 8:
            head = tokens[:2]
            last_idx = -1
            for j in range(len(tokens) - 1):
                if tokens[j : j + 2] == head:
                    last_idx = j
            if last_idx > 0 and (len(tokens) - last_idx) >= 4:
                return " ".join(tokens[last_idx:]).strip()
        return collapsed

    def _merge_into_buffer(self, text: str, norm: str) -> None:
        if not self._buffer:
            self._buffer.append(text)
            return
        last_text = self._buffer[-1]
        last_norm = _normalize_for_match(last_text)
        if not last_norm:
            self._buffer[-1] = text
            return
        if norm and (norm in last_norm) and len(norm) <= len(last_norm):
            return
        if norm and last_norm and (norm in last_norm or last_norm in norm):
            if len(norm) >= len(last_norm):
                self._buffer[-1] = text
            return
        if norm and last_norm and (norm.startswith(last_norm) or last_norm.startswith(norm)):
            if len(norm) >= len(last_norm):
                self._buffer[-1] = text
            return
        self._buffer.append(text)

    def _cancel_turn_task(self) -> None:
        if self._turn_task and not self._turn_task.done():
            self._turn_task.cancel()
        self._turn_task = None

    def _cancel_barge_task(self) -> None:
        if self._barge_task and not self._barge_task.done():
            self._barge_task.cancel()
        self._barge_task = None

    async def _emit_turn_after_silence(self, delay_ms: Optional[int] = None):
        delay_ms = self._silence_ms if delay_ms is None else max(0, int(delay_ms))
        try:
            await asyncio.sleep(delay_ms / 1000.0)
        except asyncio.CancelledError:
            return
        now = time.monotonic()
        if self._speech_stop_ts and (now - self._speech_stop_ts) * 1000 < delay_ms:
            return
        if self._last_text_ts and (now - self._last_text_ts) * 1000 < delay_ms:
            return
        text = self._dedupe_buffer()
        self._buffer.clear()
        if not text and self._pending_interim:
            text = self._pending_interim.strip()
        self._pending_interim = None
        self._pending_interim_norm = None
        if self._call_context and getattr(self._call_context, "fast_intro_pending", False):
            if text and self._is_greeting_only(text):
                intro_text = (getattr(self._call_context, "intro_text", "") or "").strip()
                if intro_text:
                    try:
                        _record_user_text(self._call_context, text)
                        self._call_context.fast_intro_pending = False
                        self._call_context.intro_sent = True
                        _record_assistant_text(self._call_context, intro_text)
                        await self.push_frame(TTSSpeakFrame(intro_text), FrameDirection.DOWNSTREAM)
                    except Exception:
                        pass
                    self._started_emitted = False
                    self._last_interim_norm = None
                    self._last_interim_ts = None
                    return
        if not text or self._is_filler_only(text) or self._is_ack_only(text) or self._ignore_greeting(text):
            self._started_emitted = False
            self._last_interim_norm = None
            self._last_interim_ts = None
            return
        norm = _normalize_for_match(text)
        if self._last_emitted_norm and self._last_emit_ts and (now - self._last_emit_ts) < self._coalesce_sec:
            last_norm = self._last_emitted_norm
            if norm == last_norm or (last_norm and (norm in last_norm or last_norm in norm)):
                self._started_emitted = False
                return
            try:
                if SequenceMatcher(None, norm, last_norm).ratio() >= self._coalesce_sim:
                    self._started_emitted = False
                    return
            except Exception:
                pass
        if self._call_context:
            last_user_text = getattr(self._call_context, "last_user_text", "") or ""
            last_user_norm = _normalize_for_match(last_user_text)
            if last_user_norm and (now - (getattr(self._call_context, "last_user_final_ts", 0) or 0)) < self._coalesce_sec:
                if norm == last_user_norm or (last_user_norm and (norm in last_user_norm or last_user_norm in norm)):
                    self._started_emitted = False
                    return
                try:
                    if SequenceMatcher(None, norm, last_user_norm).ratio() >= max(0.92, self._coalesce_sim):
                        self._started_emitted = False
                        return
                except Exception:
                    pass
        if self._call_context:
            self._call_context.last_user_final_ts = time.monotonic()
            self._call_context.conversation_state = "THINKING"
        try:
            if not self._started_emitted:
                await self.push_frame(UserStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)
            await self.push_frame(
                TranscriptionFrame(
                    text=text,
                    user_id="user",
                    timestamp=datetime.utcnow().isoformat(),
                    language=None,
                    result=None,
                ),
                FrameDirection.DOWNSTREAM,
            )
            await self.push_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        except Exception:
            pass
        self._last_emitted_norm = norm
        self._last_emit_ts = now
        self._speaking = False
        self._started_emitted = False
        self._last_interim_norm = None
        self._last_interim_ts = None

    async def _trigger_barge_in(self):
        try:
            await asyncio.sleep(self._min_speech_ms / 1000.0)
        except asyncio.CancelledError:
            return
        deadline = time.monotonic() + max(0.2, self._barge_max_wait_sec)
        while time.monotonic() < deadline:
            if not self._speaking:
                return
            if not self._call_context or not getattr(self._call_context, "tts_speaking", False):
                return
            now = time.monotonic()
            tts_start_ts = (
                getattr(self._call_context, "last_tts_first_audio_ts", 0)
                or getattr(self._call_context, "last_tts_start_ts", 0)
                or 0
            )
            if tts_start_ts and (now - tts_start_ts) < self._barge_ignore_after_tts_sec:
                await asyncio.sleep(self._barge_poll_sec)
                continue
            candidate = None
            if self._last_interim_norm and self._last_interim_ts:
                if (now - self._last_interim_ts) <= self._barge_interim_max_age:
                    candidate = self._last_interim_norm
            if not candidate and self._pending_interim_norm and self._last_interim_ts:
                if (now - self._last_interim_ts) <= self._barge_interim_max_age:
                    candidate = self._pending_interim_norm
            if not candidate and self._pending_interim and self._last_interim_ts:
                if (now - self._last_interim_ts) <= self._barge_interim_max_age:
                    candidate = _normalize_for_match(self._pending_interim or "")
            if not candidate and self._buffer:
                candidate = _normalize_for_match(" ".join(self._buffer))
            if (
                candidate
                and self._has_meaningful_words(candidate)
                and not self._is_ack_only(candidate)
                and self._barge_threshold_met(candidate)
            ):
                self._call_context.conversation_state = "LISTENING"
                try:
                    await self.push_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
                except Exception:
                    pass
                return
            await asyncio.sleep(self._barge_poll_sec)

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(
            frame,
            (UserStartedSpeakingFrame, EmulateUserStartedSpeakingFrame, VADUserStartedSpeakingFrame),
        ):
            self._vad_seen = True
            self._speaking = True
            self._speech_start_ts = time.monotonic()
            self._speech_stop_ts = None
            self._cancel_turn_task()
            self._cancel_barge_task()
            self._pending_start = True
            return
        if isinstance(
            frame,
            (UserStoppedSpeakingFrame, EmulateUserStoppedSpeakingFrame, VADUserStoppedSpeakingFrame),
        ):
            self._vad_seen = True
            self._speaking = False
            self._speech_stop_ts = time.monotonic()
            self._cancel_barge_task()
            if self._speech_start_ts:
                speech_ms = int((self._speech_stop_ts - self._speech_start_ts) * 1000)
                if speech_ms < self._min_speech_ms:
                    self._buffer.clear()
                    self._pending_start = False
                    self._started_emitted = False
                    self._pending_interim = None
                    self._pending_interim_norm = None
                    self._last_interim_norm = None
                    self._last_interim_ts = None
                    return
            if not self._buffer or self._is_filler_only(" ".join(self._buffer)) or self._is_ack_only(" ".join(self._buffer)):
                self._buffer.clear()
                self._pending_start = False
                self._started_emitted = False
                self._pending_interim = None
                self._pending_interim_norm = None
                self._last_interim_norm = None
                self._last_interim_ts = None
                return
            self._cancel_turn_task()
            self._turn_task = asyncio.create_task(self._emit_turn_after_silence())
            return
        if isinstance(frame, (InterimTranscriptionFrame, TranscriptionFrame)):
            text = (frame.text or "").strip()
            if text:
                if self._ignore_greeting(text):
                    self._pending_interim = None
                    self._pending_interim_norm = None
                    return
                now_ts = time.monotonic()
                norm = _normalize_for_match(text)
                if self._is_ack_only(text):
                    return
                self._last_text_ts = now_ts
                if isinstance(frame, InterimTranscriptionFrame):
                    self._pending_interim = text
                    self._pending_interim_norm = norm
                    self._last_interim_norm = norm
                    self._last_interim_ts = now_ts
                    if not self._speech_start_ts and not self._vad_seen:
                        self._speech_start_ts = now_ts
                        self._pending_start = True
                    if self._call_context and getattr(self._call_context, "tts_speaking", False):
                        if (
                            self._has_meaningful_words(text)
                            and not self._is_ack_only(text)
                            and self._barge_threshold_met(text)
                            and not self._barge_task
                        ):
                            self._barge_task = asyncio.create_task(self._trigger_barge_in())
                    return
                if not self._speech_start_ts and not self._vad_seen:
                    self._speech_start_ts = now_ts
                    self._pending_start = True
                if norm and self._last_norm_text == norm and self._last_norm_ts and (now_ts - self._last_norm_ts) < 1.2:
                    return
                self._merge_into_buffer(text, norm)
                self._last_norm_text = norm or self._last_norm_text
                self._last_norm_ts = now_ts
                self._pending_interim = None
                self._pending_interim_norm = None
                can_emit_start = self._has_meaningful_words(text)
                if self._call_context and getattr(self._call_context, "tts_speaking", False):
                    can_emit_start = can_emit_start and self._barge_threshold_met(text)
                if self._pending_start and can_emit_start:
                    try:
                        await self.push_frame(UserStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)
                        self._started_emitted = True
                    except Exception:
                        pass
                    self._pending_start = False
                if self._call_context and getattr(self._call_context, "tts_speaking", False):
                    if (
                        self._has_meaningful_words(text)
                        and not self._is_ack_only(text)
                        and self._barge_threshold_met(text)
                        and not self._barge_task
                    ):
                        self._barge_task = asyncio.create_task(self._trigger_barge_in())
                if not self._pending_start and not self._started_emitted and can_emit_start:
                    try:
                        await self.push_frame(UserStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)
                        self._started_emitted = True
                    except Exception:
                        pass
                self._cancel_turn_task()
                if self._call_context and getattr(self._call_context, "fast_intro_pending", False) and self._is_greeting_only(text):
                    self._turn_task = asyncio.create_task(self._emit_turn_after_silence(delay_ms=0))
                    return
                if self._vad_seen:
                    if self._speech_stop_ts and not self._speaking:
                        self._turn_task = asyncio.create_task(
                            self._emit_turn_after_silence(delay_ms=min(self._silence_ms, self._final_silence_ms))
                        )
                else:
                    self._turn_task = asyncio.create_task(self._emit_turn_after_silence())
            return
        await self.push_frame(frame, direction)


class _FastTextAggregator(BaseTextAggregator):
    def __init__(self, chunk_chars: int = 40, min_chars: int = 12):
        self._text = ""
        self._chunk_chars = max(12, chunk_chars)
        self._min_chars = max(6, min_chars)
        self._soft_punct = {",", ";", ":"}
        self._allow_soft_split = os.getenv("LLM_TEXT_SOFT_SPLIT", "false").lower() == "true"

    @property
    def text(self) -> Aggregation:
        return Aggregation(text=self._text.strip(" "), type=AggregationType.SENTENCE)

    async def aggregate(self, text: str):
        for char in text:
            self._text += char
            if self._text and self._text[-1] in ".?!":
                result = self._text.strip(" ")
                self._text = ""
                yield Aggregation(text=result, type=AggregationType.SENTENCE)
                continue
            if self._allow_soft_split and len(self._text) >= self._chunk_chars and char.isspace():
                # Optional soft split for latency; off by default to prevent mid-sentence pauses.
                last_soft = max((self._text.rfind(p) for p in self._soft_punct), default=-1)
                if last_soft >= self._min_chars:
                    result = self._text[: last_soft + 1].strip(" ")
                    remainder = self._text[last_soft + 1 :].lstrip(" ")
                    self._text = remainder
                    if result:
                        yield Aggregation(text=result, type=AggregationType.SENTENCE)

    async def flush(self):
        if self._text:
            result = self._text.strip(" ")
            self._text = ""
            return Aggregation(text=result, type=AggregationType.SENTENCE)
        return None

    async def handle_interruption(self):
        self._text = ""

    async def reset(self):
        self._text = ""


class _AssistantTextLogger(FrameProcessor):
    def __init__(self, call_context: CallContext):
        super().__init__()
        self._call_context = call_context

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, AggregatedTextFrame):
            text = (frame.text or "").strip()
            if text and self._call_context:
                if not getattr(self._call_context, "conversation_history", None):
                    self._call_context.conversation_history = []
                self._call_context.conversation_history.append({"speaker": "assistant", "message": text})
                self._call_context.last_assistant_message = text
                if text.endswith("?"):
                    self._call_context.last_assistant_question = text
                else:
                    self._call_context.last_assistant_question = None
                if not getattr(self._call_context, "user_context", None):
                    self._call_context.user_context = []
                self._call_context.user_context.append({"role": "assistant", "content": text})
                self._call_context.assistant_has_spoken = True
                if not getattr(self._call_context, "intro_sent", False):
                    self._call_context.intro_sent = True
                    self._call_context.fast_intro_pending = False
                save_call_context(self._call_context.call_sid, self._call_context)
                save_to_log(self._call_context.call_sid, "assistant", text)
        await self.push_frame(frame, direction)


class _OneSentenceLimiter(FrameProcessor):
    """Allow only one sentence per LLM response to keep TTS streams short."""

    def __init__(self):
        super().__init__()
        self._sent_this_turn = False
        self._buffer = ""
        self._agg_by = AggregationType.SENTENCE
        self._abbreviations = {
            "mr",
            "mrs",
            "ms",
            "dr",
            "prof",
            "sr",
            "jr",
            "st",
            "vs",
            "etc",
            "e.g",
            "i.e",
            "sq",
            "ft",
            "cr",
            "lakh",
            "lakhs",
            "apt",
            "no",
        }

    def _is_sentence_end(self, text: str, idx: int) -> bool:
        ch = text[idx]
        if ch in "?!":
            return True
        if ch != ".":
            return False
        prev_ch = text[idx - 1] if idx > 0 else ""
        next_ch = text[idx + 1] if (idx + 1) < len(text) else ""
        # Don't split decimal values like 1.69.
        if prev_ch.isdigit() and next_ch.isdigit():
            return False
        # Don't split ellipsis pieces.
        if next_ch == ".":
            return False
        j = idx - 1
        while j >= 0 and text[j].isspace():
            j -= 1
        end = j + 1
        while j >= 0 and text[j].isalpha():
            j -= 1
        token = text[j + 1 : end].lower()
        if token in self._abbreviations:
            return False
        return True

    def _split_sentences(self, text: str) -> list[str]:
        sentences: list[str] = []
        buf = ""
        for idx, ch in enumerate(text):
            buf += ch
            if self._is_sentence_end(text, idx):
                sentence = buf.strip()
                if sentence:
                    sentences.append(sentence)
                buf = ""
                if len(sentences) >= 2:
                    break
        if buf and not sentences:
            # No terminal punctuation yet.
            return []
        return sentences

    @staticmethod
    def _merge_two_sentences(first: str, second: str) -> str:
        first = first.strip()
        second = second.strip()
        if not first:
            return second
        if not second:
            return first
        joiner = ", "
        if first.endswith((".", "!", "?")):
            first = first[:-1].strip()
        if second and second[0].isupper():
            second = second[0].lower() + second[1:]
        return f"{first}{joiner}{second}"

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, LLMFullResponseStartFrame):
            self._sent_this_turn = False
            self._buffer = ""
            self._agg_by = AggregationType.SENTENCE
            await self.push_frame(frame, direction)
            return
        if isinstance(frame, AggregatedTextFrame):
            if self._sent_this_turn:
                return
            self._agg_by = getattr(frame, "aggregated_by", AggregationType.SENTENCE)
            text = frame.text or ""
            if not text.strip():
                return
            if self._buffer and not self._buffer.endswith((" ", "\n")) and not text.startswith((" ", "\n")):
                self._buffer += " "
            self._buffer += text
            sentences = self._split_sentences(self._buffer)
            if sentences:
                first = sentences[0]
                second = sentences[1] if len(sentences) > 1 else ""
                emit_text = first
                # If the first sentence isn't a question and we have a second, merge
                # them into one sentence so the user still hears the question.
                if "?" not in first and second:
                    emit_text = self._merge_two_sentences(first, second)
                self._sent_this_turn = True
                self._buffer = ""
                await self.push_frame(
                    AggregatedTextFrame(emit_text.strip(), self._agg_by), direction
                )
            return
        if isinstance(frame, LLMFullResponseEndFrame):
            if not self._sent_this_turn:
                pending = self._buffer.strip()
                if pending:
                    if pending[-1] not in ".?!":
                        pending += "."
                    await self.push_frame(
                        AggregatedTextFrame(pending, self._agg_by), direction
                    )
                    self._sent_this_turn = True
            self._buffer = ""
            await self.push_frame(frame, direction)
            return
        await self.push_frame(frame, direction)


class _AssistantOutputDeduper(FrameProcessor):
    def __init__(self, call_context: CallContext):
        super().__init__()
        self._call_context = call_context
        self._window_sec = float(os.getenv("ASSISTANT_DEDUPE_WINDOW_SEC", "6.0"))
        self._similarity = float(os.getenv("ASSISTANT_DEDUPE_SIMILARITY", "0.85"))
        self._last_norm = ""
        self._last_ts = 0.0

    @staticmethod
    def _normalize(text: str) -> str:
        return _normalize_for_match(text)

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        text = None
        if isinstance(frame, (AggregatedTextFrame, TTSTextFrame, TTSSpeakFrame)):
            text = getattr(frame, "text", None)
        if not text:
            await self.push_frame(frame, direction)
            return

        norm = self._normalize(text)
        if not norm:
            await self.push_frame(frame, direction)
            return

        now = time.monotonic()
        last_user_ts = getattr(self._call_context, "last_user_final_ts", 0) or 0
        last_assistant_ts = getattr(self._call_context, "last_assistant_utterance_ts", 0) or 0
        if last_user_ts > last_assistant_ts:
            self._last_norm = ""
            self._last_ts = 0.0

        if self._last_norm and (now - self._last_ts) < self._window_sec:
            if norm == self._last_norm:
                return
            if norm in self._last_norm or self._last_norm in norm:
                return
            try:
                ratio = SequenceMatcher(None, norm, self._last_norm).ratio()
                len_diff = abs(len(norm) - len(self._last_norm)) / max(len(norm), len(self._last_norm))
                if ratio >= self._similarity and len_diff < 0.25:
                    return
            except Exception:
                pass

        self._last_norm = norm
        self._last_ts = now
        if self._call_context:
            self._call_context.last_assistant_utterance_ts = now
        await self.push_frame(frame, direction)


class _AssistantSafetyGuard(FrameProcessor):
    """Strip non-spoken control markers from assistant text before TTS."""

    def __init__(self, call_context: CallContext, llm_context: Optional[LLMContext] = None):
        super().__init__()
        self._call_context = call_context
        self._llm_context = llm_context
        self._blocked_control_phrases = {
            "end call",
            "call end",
            "transfer call",
        }

    @staticmethod
    def _split_sentences_for_filter(text: str) -> List[str]:
        if not text:
            return []
        parts = re.split(r"(?<=[.!?])\s+|\n+", text)
        return [p.strip() for p in parts if p and p.strip()]

    def _sanitize_text(self, text: str) -> str:
        if not text:
            return ""
        # Remove stand-alone control marker lines first.
        text = re.sub(
            r"(?im)^\s*[\(\[\{\"\']?\s*(?:end\s+call|call\s+end|transfer\s+call)\s*[\)\]\}\"\'\.\!\?]?\s*$",
            "",
            text,
        )
        kept: List[str] = []
        for sentence in self._split_sentences_for_filter(text):
            norm = _normalize_for_match(sentence)
            if norm in self._blocked_control_phrases:
                continue
            kept.append(sentence)
        out = " ".join(kept).strip()
        out = re.sub(r"\s{2,}", " ", out)
        return out

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, (AggregatedTextFrame, TTSTextFrame, TTSSpeakFrame)):
            text = (getattr(frame, "text", "") or "").strip()
            if not text:
                await self.push_frame(frame, direction)
                return
            sanitized = self._sanitize_text(text)
            if not sanitized:
                logger.info(
                    "Blocked assistant control marker from speech (CallSid={}).",
                    getattr(self._call_context, "call_sid", ""),
                )
                return
            if sanitized != text:
                logger.info(
                    "Sanitized assistant control marker from speech (CallSid={}).",
                    getattr(self._call_context, "call_sid", ""),
                )
                if isinstance(frame, AggregatedTextFrame):
                    out = AggregatedTextFrame(sanitized, getattr(frame, "aggregated_by", AggregationType.SENTENCE))
                    out.skip_tts = getattr(frame, "skip_tts", False)
                    await self.push_frame(out, direction)
                    return
                if isinstance(frame, TTSTextFrame):
                    out = TTSTextFrame(sanitized)
                    out.skip_tts = getattr(frame, "skip_tts", False)
                    await self.push_frame(out, direction)
                    return
                if isinstance(frame, TTSSpeakFrame):
                    out = TTSSpeakFrame(sanitized)
                    out.skip_tts = getattr(frame, "skip_tts", False)
                    await self.push_frame(out, direction)
                    return
        await self.push_frame(frame, direction)


class _AssistantSentenceChunker(FrameProcessor):
    def __init__(self):
        super().__init__()
        self._max_chars = int(os.getenv("TTS_CHUNK_MAX_CHARS", "200"))

    @staticmethod
    def _split_sentences(text: str) -> List[str]:
        parts = re.findall(r"[^.!?]+[.!?]?", text.strip())
        return [p.strip() for p in parts if p.strip()]

    def _split_long(self, sentence: str) -> List[str]:
        if len(sentence) <= self._max_chars:
            return [sentence]
        chunks: List[str] = []
        buf = ""
        for part in sentence.split(","):
            part = part.strip()
            if not part:
                continue
            candidate = f"{buf}, {part}" if buf else part
            if len(candidate) <= self._max_chars:
                buf = candidate
            else:
                if buf:
                    chunks.append(buf.strip())
                buf = part
        if buf:
            chunks.append(buf.strip())
        final: List[str] = []
        for chunk in chunks:
            if len(chunk) <= self._max_chars:
                final.append(chunk)
                continue
            words = chunk.split()
            buf = ""
            for word in words:
                candidate = f"{buf} {word}".strip() if buf else word
                if len(candidate) <= self._max_chars:
                    buf = candidate
                else:
                    if buf:
                        final.append(buf)
                    buf = word
            if buf:
                final.append(buf)
        return final or [sentence]

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, (AggregatedTextFrame, TTSTextFrame, TTSSpeakFrame)):
            text = getattr(frame, "text", "") or ""
            if not text.strip():
                await self.push_frame(frame, direction)
                return
            sentences = self._split_sentences(text)
            chunks: List[str] = []
            for sentence in sentences:
                chunks.extend(self._split_long(sentence))
            if len(chunks) <= 1:
                await self.push_frame(frame, direction)
                return
            skip_tts = getattr(frame, "skip_tts", False)
            for chunk in chunks:
                out = AggregatedTextFrame(chunk, AggregationType.SENTENCE)
                out.skip_tts = skip_tts
                await self.push_frame(out, direction)
            return
        await self.push_frame(frame, direction)


class _AssistantSpokenLogger(FrameProcessor):
    """Log actual assistant speech text that is sent to TTS."""

    def __init__(self, call_context: CallContext):
        super().__init__()
        self._call_context = call_context
        self._last_norm = ""
        self._last_ts = 0.0
        self._window_sec = float(os.getenv("ASSISTANT_SPOKEN_LOG_DEDUPE_SEC", "1.5"))

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, (AggregatedTextFrame, TTSTextFrame, TTSSpeakFrame)):
            text = (getattr(frame, "text", "") or "").strip()
            skip_tts = bool(getattr(frame, "skip_tts", False))
            if text and not skip_tts and self._call_context and self._call_context.call_sid:
                norm = _normalize_for_match(text)
                now = time.monotonic()
                if not (
                    norm
                    and norm == self._last_norm
                    and (now - self._last_ts) < self._window_sec
                ):
                    self._last_norm = norm
                    self._last_ts = now
                    save_to_log(self._call_context.call_sid, "assistant", text)
        await self.push_frame(frame, direction)


class _AssistantContextTracker(FrameProcessor):
    def __init__(self, call_context: CallContext, llm_context: Optional[LLMContext] = None):
        super().__init__()
        self._call_context = call_context
        self._llm_context = llm_context
        self._aggregation = ""
        self._started = False

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, StartFrame):
            # Ensure this processor is marked started before forwarding StartFrame.
            if not getattr(self, "_FrameProcessor__started", False):
                setattr(self, "_FrameProcessor__started", True)
            await self.push_frame(frame, direction)
            return
        if isinstance(frame, LLMFullResponseStartFrame):
            self._started = True
            self._aggregation = ""
            if self._call_context:
                now = time.monotonic()
                self._call_context.llm_response_started_ts = now
                last_user_ts = getattr(self._call_context, "last_user_final_ts", None)
                if last_user_ts:
                    stt_to_llm_ms = int((now - last_user_ts) * 1000)
                    logger.info("Latency STT->LLM start: {}ms", stt_to_llm_ms)
        elif isinstance(frame, LLMTextFrame):
            if self._started and frame.text:
                self._aggregation += frame.text
                if self._call_context:
                    self._call_context.last_llm_text_ts = time.monotonic()
        elif isinstance(frame, LLMFullResponseEndFrame):
            if self._started:
                text = self._aggregation.strip()
                if text:
                    try:
                        logger.info("LLM output: {}", text)
                    except Exception:
                        pass
                    if self._llm_context is not None:
                        self._llm_context.add_message({"role": "assistant", "content": text})
                    if self._call_context:
                        if not getattr(self._call_context, "conversation_history", None):
                            self._call_context.conversation_history = []
                        self._call_context.conversation_history.append(
                            {"speaker": "assistant", "message": text}
                        )
                        self._call_context.last_assistant_message = text
                        if text.endswith("?"):
                            self._call_context.last_assistant_question = text
                        else:
                            self._call_context.last_assistant_question = None
                        if not getattr(self._call_context, "user_context", None):
                            self._call_context.user_context = []
                        self._call_context.user_context.append(
                            {"role": "assistant", "content": text}
                        )
                        self._call_context.assistant_has_spoken = True
                        if not getattr(self._call_context, "intro_sent", False):
                            self._call_context.intro_sent = True
                        if getattr(self._call_context, "fast_intro_pending", False):
                            self._call_context.fast_intro_pending = False
                        save_call_context(self._call_context.call_sid, self._call_context)
                        self._call_context.last_assistant_utterance_ts = time.monotonic()
                        end_ts = time.monotonic()
                        self._call_context.last_llm_response_end_ts = end_ts
                        self._call_context.awaiting_tts_first_audio = True
                        if _assistant_matches_prompt_closing(text, self._call_context):
                            if not getattr(self._call_context, "auto_end_scheduled", False):
                                setattr(self._call_context, "auto_end_scheduled", True)
                                logger.info(
                                    "Detected prompt-driven end-call phrase (CallSid={}); scheduling end_call.",
                                    self._call_context.call_sid,
                                )
                                asyncio.create_task(
                                    _end_call_after_assistant_closing(self._call_context, text)
                                )
                        start_ts = getattr(self._call_context, "llm_response_started_ts", None)
                        if start_ts:
                            llm_ms = int((end_ts - start_ts) * 1000)
                            logger.info("Latency LLM processing: {}ms", llm_ms)
                self._started = False
                self._aggregation = ""
        elif isinstance(frame, InterruptionFrame):
            self._started = False
            self._aggregation = ""

        await self.push_frame(frame, direction)


class _LatencyLogger(FrameProcessor):
    def __init__(self, call_context: CallContext):
        super().__init__()
        self._call_context = call_context

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, TTSAudioRawFrame):
            if self._call_context and getattr(self._call_context, "awaiting_tts_first_audio", False):
                now = time.monotonic()
                llm_end_ts = getattr(self._call_context, "last_llm_response_end_ts", None)
                user_ts = getattr(self._call_context, "last_user_final_ts", None)
                if llm_end_ts:
                    ttfb_ms = int((now - llm_end_ts) * 1000)
                    logger.info("Latency LLM->TTS first audio: {}ms", ttfb_ms)
                if user_ts:
                    e2e_ms = int((now - user_ts) * 1000)
                    logger.info("Latency end-to-end (user end -> audio): {}ms", e2e_ms)
                self._call_context.awaiting_tts_first_audio = False
        await self.push_frame(frame, direction)


class _TTSSpeakingTracker(FrameProcessor):
    def __init__(self, call_context: CallContext):
        super().__init__()
        self._call_context = call_context
        self._last_audio_ts: Optional[float] = None
        self._idle_sec = float(os.getenv("TTS_SPEAKING_IDLE_SEC", "0.6"))

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if not self._call_context:
            await self.push_frame(frame, direction)
            return
        now = time.monotonic()
        if self._call_context.tts_speaking and self._last_audio_ts:
            if (now - self._last_audio_ts) > self._idle_sec:
                self._call_context.tts_speaking = False
        if isinstance(frame, (TTSStartedFrame, BotStartedSpeakingFrame)):
            self._call_context.tts_speaking = True
            self._call_context.last_tts_start_ts = time.monotonic()
        elif isinstance(frame, (TTSStoppedFrame, BotStoppedSpeakingFrame)):
            self._call_context.tts_speaking = False
            self._call_context.last_tts_stop_ts = time.monotonic()
        elif isinstance(frame, TTSAudioRawFrame):
            self._call_context.tts_speaking = True
            self._last_audio_ts = now
            self._call_context.last_tts_audio_ts = now
        await self.push_frame(frame, direction)


class StableElevenLabsTTSService(ElevenLabsTTSService):
    """ElevenLabs TTS with longer audio-context timeout to prevent mid-sentence truncation."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._audio_context_timeout = float(
            os.getenv("TTS_AUDIO_CONTEXT_TIMEOUT_SEC", "4.0")
        )
        self._audio_context_max_idle = float(
            os.getenv("TTS_AUDIO_CONTEXT_MAX_IDLE_SEC", "20.0")
        )
        self._pending_by_context: Dict[str, str] = {}
        self._retry_by_context: Dict[str, int] = {}
        self._context_has_audio: Dict[str, bool] = {}
        self._last_text_by_context: Dict[str, str] = {}
        self._last_text: Optional[str] = None
        self._max_retries = int(os.getenv("TTS_CONTEXT_MAX_RETRIES", "1"))

    async def _push_tts_frames(
        self, src_frame: AggregatedTextFrame, includes_inter_frame_spaces: Optional[bool] = False
    ):
        text = getattr(src_frame, "text", "") or ""
        if text.strip():
            self._last_text = text
            ctx = getattr(self, "_context_id", None)
            if ctx:
                self._pending_by_context[ctx] = text
                self._retry_by_context.setdefault(ctx, 0)
                self._last_text_by_context[ctx] = text
        return await super()._push_tts_frames(src_frame, includes_inter_frame_spaces)

    async def _handle_audio_context(self, context_id: str):
        queue = self._contexts[context_id]
        last_audio_ts = time.monotonic()
        self._context_has_audio.setdefault(context_id, False)
        running = True
        while running:
            try:
                frame = await asyncio.wait_for(queue.get(), timeout=self._audio_context_timeout)
                if frame:
                    last_audio_ts = time.monotonic()
                    self._context_has_audio[context_id] = True
                    await self.push_frame(frame)
                running = frame is not None
            except asyncio.TimeoutError:
                idle = time.monotonic() - last_audio_ts
                if idle < self._audio_context_max_idle:
                    # Keep waiting; short gaps between chunks should not truncate speech.
                    continue
                pending_text = self._pending_by_context.get(context_id)
                if not pending_text:
                    pending_text = self._last_text_by_context.get(context_id)
                if not pending_text:
                    pending_text = self._last_text
                if pending_text:
                    retries = self._retry_by_context.get(context_id, 0)
                    if self._context_has_audio.get(context_id, False):
                        logger.debug(
                            "TTS audio context {} reached idle timeout after audio; closing context.",
                            context_id,
                        )
                        try:
                            if self._websocket:
                                await self._websocket.send(
                                    json.dumps({"context_id": context_id, "close_context": True})
                                )
                        except Exception:
                            pass
                        break
                    if retries < self._max_retries:
                        self._retry_by_context[context_id] = retries + 1
                        logger.debug(
                            "TTS audio context {} stalled for {}ms; retrying text.",
                            context_id,
                            int(idle * 1000),
                        )
                        try:
                            if self._websocket:
                                await self._websocket.send(
                                    json.dumps({"context_id": context_id, "close_context": True})
                                )
                            self._context_id = None
                            await self._connect()
                            self._context_id = str(uuid.uuid4())
                            await self.create_audio_context(self._context_id)
                            init_msg = {"text": " ", "context_id": self._context_id}
                            if getattr(self, "_voice_settings", None):
                                init_msg["voice_settings"] = self._voice_settings
                            if getattr(self, "_pronunciation_dictionary_locators", None):
                                init_msg["pronunciation_dictionary_locators"] = [
                                    locator.model_dump()
                                    for locator in self._pronunciation_dictionary_locators
                                ]
                            await self._websocket.send(json.dumps(init_msg))
                            await self._send_text(pending_text)
                            self._pending_by_context[self._context_id] = pending_text
                        except Exception:
                            logger.debug(
                                "TTS retry failed for context {}.",
                                context_id,
                            )
                        break
                    logger.debug(
                        "TTS audio context {} stalled for {}ms; giving up after {} retries.",
                        context_id,
                        int(idle * 1000),
                        retries,
                    )
                    break
                logger.debug(
                    "TTS audio context {} stalled for {}ms; no pending text to retry.",
                    context_id,
                    int(idle * 1000),
                )
                break

        # Context completed; clear pending text for this context.
        self._pending_by_context.pop(context_id, None)
        self._retry_by_context.pop(context_id, None)
        self._context_has_audio.pop(context_id, None)
        self._last_text_by_context.pop(context_id, None)

    async def run_tts(self, text: str):
        # Track pending text per context so we can retry if audio stalls.
        async for frame in super().run_tts(text):
            if isinstance(frame, TTSStartedFrame):
                ctx = getattr(self, "_context_id", None)
                if ctx:
                    self._pending_by_context[ctx] = text
                    self._retry_by_context.setdefault(ctx, 0)
                    self._last_text_by_context[ctx] = text
            if frame is None:
                ctx = getattr(self, "_context_id", None)
                if ctx:
                    self._pending_by_context[ctx] = text
                    self._retry_by_context.setdefault(ctx, 0)
                    self._last_text_by_context[ctx] = text
            yield frame


def _build_tools_schema() -> ToolsSchema:
    standard_tools: List[FunctionSchema] = []
    for tool in function_tools:
        fn = tool.get("function") if isinstance(tool, dict) else None
        if not fn:
            continue
        name = fn.get("name")
        description = fn.get("description", "")
        params = fn.get("parameters", {}) or {}
        properties = params.get("properties", {}) or {}
        required = params.get("required", []) or []
        if name:
            standard_tools.append(
                FunctionSchema(
                    name=name,
                    description=description,
                    properties=properties,
                    required=required,
                )
            )
    return ToolsSchema(standard_tools=standard_tools)


def _strip_control_markers(text: str) -> str:
    if not text:
        return text
    lines = []
    for line in text.splitlines():
        if "→" in line:
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def _compose_system_prompt(agent_prompt: str) -> str:
    global_prompt = read_global_prompt().strip()
    parts = []
    if global_prompt:
        parts.append(global_prompt)
    if agent_prompt:
        parts.append(_strip_control_markers(agent_prompt.strip()))
    return "\n\n".join(parts).strip()


def _extract_intro_from_prompt(prompt: str) -> str:
    if not prompt:
        return ""
    cleaned = _strip_control_markers(prompt)
    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    if not lines:
        return ""
    lower_lines = [line.lower() for line in lines]
    start_idx = None
    for idx, line in enumerate(lower_lines):
        if line.startswith("introduction"):
            start_idx = idx + 1
            break
    stop_prefixes = (
        "if customer",
        "if user",
        "project overview",
        "overview",
        "configurations",
        "pricing",
        "qualification",
        "transfer",
        "closing",
    )
    intro_lines: List[str] = []
    if start_idx is not None:
        for line in lines[start_idx:]:
            lower = line.lower()
            if any(lower.startswith(prefix) for prefix in stop_prefixes):
                break
            if is_all_caps_heading(line):
                break
            intro_lines.append(line)
    if not intro_lines:
        for line in lines:
            if "?" in line:
                intro_lines = [line]
                break
    if not intro_lines:
        return ""
    text = " ".join(intro_lines).strip()
    if not text:
        return ""
    sentences = []
    buf = ""
    for ch in text:
        buf += ch
        if ch in ".?!":
            sentences.append(buf.strip())
            buf = ""
            if len(sentences) >= 2:
                break
    if buf and len(sentences) < 2:
        sentences.append(buf.strip())
    return " ".join(sentences).strip()


def _extract_project_overview_from_prompt(prompt: str) -> str:
    if not prompt:
        return ""
    cleaned = _strip_control_markers(prompt)
    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    if not lines:
        return ""
    lower_lines = [line.lower() for line in lines]
    start_idx = None
    for idx, line in enumerate(lower_lines):
        if line.startswith("project overview") or line.startswith("overview"):
            start_idx = idx + 1
            break
    if start_idx is None:
        return ""
    stop_prefixes = (
        "configurations",
        "pricing",
        "qualification",
        "transfer",
        "closing",
        "if customer",
        "if user",
    )
    overview_lines: List[str] = []
    for line in lines[start_idx:]:
        lower = line.lower()
        if any(lower.startswith(prefix) for prefix in stop_prefixes):
            break
        if is_all_caps_heading(line):
            break
        if line.endswith(":"):
            continue
        overview_lines.append(line)
    text = " ".join(overview_lines).strip()
    return text


def _extract_closing_from_prompt(prompt: str) -> str:
    if not prompt:
        return ""
    cleaned = _strip_control_markers(prompt)
    lines = [line.strip().strip('"') for line in cleaned.splitlines() if line.strip()]
    if not lines:
        return ""

    lower_lines = [line.lower() for line in lines]
    start_idx = None
    for idx, line in enumerate(lower_lines):
        if line.startswith("closing"):
            start_idx = idx + 1
            break
    if start_idx is None:
        return ""

    stop_prefixes = (
        "end call",
        "if customer",
        "if user",
        "project overview",
        "overview",
        "configurations",
        "pricing",
        "qualification",
        "transfer",
        "introduction",
        "system role",
        "hard rules",
        "fixed facts",
    )
    closing_lines: List[str] = []
    for line in lines[start_idx:]:
        lower = line.lower()
        if any(lower.startswith(prefix) for prefix in stop_prefixes):
            break
        if is_all_caps_heading(line):
            break
        if line in {"(wait)", "wait"}:
            continue
        closing_lines.append(line)

    text = " ".join(closing_lines).strip()
    if not text:
        return ""
    text = " ".join(text.split())
    return text


def _looks_like_prompt_heading(line: str) -> bool:
    if not line:
        return False
    normalized = line.strip()
    lower = normalized.lower().strip(":")
    if not lower:
        return False
    if is_all_caps_heading(normalized):
        return True
    heading_prefixes = (
        "system role",
        "hard rules",
        "fixed facts",
        "introduction",
        "if customer",
        "if user",
        "project overview",
        "overview",
        "configurations",
        "pricing",
        "qualification",
        "transfer",
        "closing",
        "prices",
        "areas",
        "eoi",
        "tool usage",
        "tools",
    )
    if any(lower.startswith(prefix) for prefix in heading_prefixes):
        return True
    if normalized.endswith(":") and len(lower.split()) <= 8:
        return True
    return False


def _extract_end_call_phrases_from_prompt(prompt: str) -> List[str]:
    if not prompt:
        return []
    cleaned = _strip_control_markers(prompt)
    lines = [line.strip().strip('"') for line in cleaned.splitlines() if line.strip()]
    if not lines:
        return []

    section_lines: List[str] = []
    phrases: List[str] = []

    def _flush_section() -> None:
        nonlocal section_lines
        if not section_lines:
            return
        text = " ".join(section_lines).strip()
        text = " ".join(text.split())
        if len(text.split()) >= 4 and text not in phrases:
            phrases.append(text)
        section_lines = []

    for line in lines:
        lower = line.lower().strip()
        if lower in {"(wait)", "wait"}:
            continue
        if lower.startswith("end call"):
            _flush_section()
            continue
        if _looks_like_prompt_heading(line):
            section_lines = []
            continue
        section_lines.append(line)

    closing_text = _extract_closing_from_prompt(prompt)
    if closing_text and closing_text not in phrases:
        phrases.append(closing_text)
    return phrases


def _text_matches_reference(text: str, reference: str) -> bool:
    assistant_norm = _normalize_for_match(text)
    reference_norm = _normalize_for_match(reference)
    if not assistant_norm or not reference_norm:
        return False
    if reference_norm in assistant_norm:
        return True
    if assistant_norm in reference_norm and len(assistant_norm.split()) >= 6:
        return True

    similarity = float(os.getenv("PROMPT_CLOSING_MATCH_SIMILARITY", "0.72"))
    try:
        if SequenceMatcher(None, assistant_norm, reference_norm).ratio() >= similarity:
            return True
    except Exception:
        pass

    for sentence in re.split(r"[.?!]\s*", reference):
        sent_norm = _normalize_for_match(sentence)
        if len(sent_norm.split()) < 4:
            continue
        if sent_norm in assistant_norm:
            return True
    return False


def _assistant_matches_prompt_closing(text: str, call_context: Optional[CallContext]) -> bool:
    if not text or not call_context:
        return False
    prompt_end_call_phrases = list(getattr(call_context, "end_call_phrases", None) or [])
    closing_text = (getattr(call_context, "closing_text", "") or "").strip()
    if closing_text:
        prompt_end_call_phrases.append(closing_text)
    if not prompt_end_call_phrases:
        return False
    deduped: List[str] = []
    for phrase in prompt_end_call_phrases:
        normalized = " ".join((phrase or "").split()).strip()
        if normalized and normalized not in deduped:
            deduped.append(normalized)
    for reference in deduped:
        if _text_matches_reference(text, reference):
            return True
    return False


async def _end_call_after_assistant_closing(
    call_context: Optional[CallContext],
    closing_text: str,
) -> None:
    if not call_context:
        return
    if getattr(call_context, "ending_in_progress", False):
        return
    setattr(call_context, "ending_in_progress", True)
    delay = float(os.getenv("PROMPT_AUTO_END_CALL_GRACE_SEC", "0.8"))
    await asyncio.sleep(max(0.2, delay))
    try:
        result = await end_call_func(call_context, {"farewell": closing_text, "reason": "prompt_closing"})
        logger.info("Auto end_call from prompt closing (CallSid={}): {}", call_context.call_sid, result)
    except Exception as exc:
        logger.warning("Auto end_call from prompt closing failed (CallSid={}): {}", call_context.call_sid, exc)
    finally:
        setattr(call_context, "ending_in_progress", False)
        setattr(call_context, "auto_end_scheduled", False)


def _greeting_cache_key(text: str, voice_id: str, model_id: str) -> str:
    key_src = f"{voice_id}|{model_id}|{text}".encode("utf-8")
    return hashlib.sha1(key_src).hexdigest()


def _greeting_audio_path(key: str) -> FsPath:
    return GREETING_AUDIO_DIR / f"{key}.mp3"


async def _generate_greeting_audio(text: str, voice_id: str, model_id: str, path: FsPath) -> bool:
    if not text or not voice_id:
        return False
    api_key = os.getenv("ELEVENLABS_API_KEY", "").strip()
    if not api_key:
        logger.warning("Greeting audio generation skipped: ELEVENLABS_API_KEY missing.")
        return False
    if path.exists():
        return True
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
    headers = {
        "xi-api-key": api_key,
        "Accept": "audio/mpeg",
        "Content-Type": "application/json",
    }
    payload = {"text": text, "model_id": model_id}
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                if resp.status != 200:
                    logger.warning(
                        "Greeting audio generation failed (status {}): {}",
                        resp.status,
                        await resp.text(),
                    )
                    return False
                audio_bytes = await resp.read()
        tmp_path = path.with_suffix(".mp3.tmp")
        async with aiofiles.open(tmp_path, "wb") as f:
            await f.write(audio_bytes)
        tmp_path.replace(path)
        return True
    except Exception as exc:
        logger.warning("Greeting audio generation error: {}", exc)
        return False


async def _prewarm_elevenlabs_tts(voice_id: str) -> None:
    if not voice_id:
        return
    try:
        model_id = os.getenv("ELEVENLABS_TTS_MODEL", "eleven_turbo_v2_5")
        text = os.getenv("TTS_PREWARM_TEXT", "Hello")
        if not text:
            return
        key = _greeting_cache_key(text, voice_id, model_id)
        path = _greeting_audio_path(key)
        await _generate_greeting_audio(text, voice_id, model_id, path)
    except Exception:
        return


async def _prewarm_elevenlabs_stt() -> None:
    try:
        api_key = os.getenv("ELEVENLABS_API_KEY", "").strip()
        if not api_key:
            return
        stt = ElevenLabsRealtimeSTTService(
            api_key=api_key,
            sample_rate=int(os.getenv("PIPELINE_INPUT_SAMPLE_RATE", "16000")),
            language=os.getenv("STT_PREWARM_LANG", "en"),
        )
        await stt._ensure_connected()
        await stt._disconnect()
    except Exception:
        return


async def _prewarm_openai_llm(model_name: str) -> None:
    try:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return
        model = (model_name or "").strip() or os.getenv("OPENAI_MODEL", "gpt-5.2-chat-latest")
        if model.lower().startswith("claude-"):
            return
        client = AsyncOpenAI(api_key=api_key)
        await client.responses.create(
            model=model,
            input="ping",
            max_completion_tokens=1,
        )
    except Exception:
        return


def _normalize_for_match(text: str) -> str:
    cleaned = []
    for ch in text.lower():
        if ch.isalnum() or ch.isspace():
            cleaned.append(ch)
        else:
            cleaned.append(" ")
    return " ".join("".join(cleaned).split())


def _record_user_text(call_context: CallContext, text: str) -> None:
    if not call_context or not text:
        return
    if not getattr(call_context, "conversation_history", None):
        call_context.conversation_history = []
    call_context.conversation_history.append({"speaker": "user", "message": text})
    if not getattr(call_context, "user_context", None):
        call_context.user_context = []
    call_context.user_context.append({"role": "user", "content": text})
    call_context.last_user_utterance_ts = time.monotonic()
    call_context.last_user_final_ts = time.monotonic()
    call_context.last_user_text = text
    save_call_context(call_context.call_sid, call_context)
    save_to_log(call_context.call_sid, "user", text)


def _record_assistant_text(call_context: CallContext, text: str) -> None:
    if not call_context or not text:
        return
    if not getattr(call_context, "conversation_history", None):
        call_context.conversation_history = []
    call_context.conversation_history.append({"speaker": "assistant", "message": text})
    call_context.last_assistant_message = text
    if text.endswith("?"):
        call_context.last_assistant_question = text
    else:
        call_context.last_assistant_question = None
    if not getattr(call_context, "user_context", None):
        call_context.user_context = []
    call_context.user_context.append({"role": "assistant", "content": text})
    call_context.last_assistant_utterance_ts = time.monotonic()
    call_context.assistant_has_spoken = True
    save_call_context(call_context.call_sid, call_context)


def _resolve_openai_tts_voice(candidate: str) -> str:
    allowed = {"alloy", "ash", "coral", "sage", "shimmer"}
    if candidate and candidate.lower() in allowed:
        return candidate.lower()
    return os.getenv("OPENAI_TTS_VOICE", "alloy")


async def _get_available_openai_models() -> Set[str]:
    global _OPENAI_MODEL_CACHE, _OPENAI_MODEL_CACHE_TS
    ttl = float(os.getenv("OPENAI_MODEL_CACHE_TTL_SEC", "300"))
    now = time.monotonic()
    if _OPENAI_MODEL_CACHE and (now - _OPENAI_MODEL_CACHE_TS) < ttl:
        return _OPENAI_MODEL_CACHE

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return set()
    try:
        client = AsyncOpenAI(api_key=api_key)
        models = await client.models.list()
        ids = {m.id for m in models.data if getattr(m, "id", None)}
        _OPENAI_MODEL_CACHE = ids
        _OPENAI_MODEL_CACHE_TS = now
        return ids
    except Exception as exc:
        logger.warning("Failed to list OpenAI models: {}", exc)
        return set()


async def _resolve_openai_chat_model(preferred: Optional[str] = None) -> str:
    preferred = (preferred or "").strip()
    alias_map = {
        "turbo": "gpt-5-mini",
        "fast": "gpt-4o-mini",
        "best": "gpt-5.2-chat-latest",
        "latest": "gpt-5.2-chat-latest",
    }
    preferred_key = preferred.lower()
    if preferred_key in alias_map:
        preferred = alias_map[preferred_key]
    candidates = [m for m in [preferred] if m]
    candidates += [
        "gpt-5.2-chat-latest",
        "gpt-5.1-chat-latest",
        "gpt-5-chat-latest",
        "gpt-5.2",
        "gpt-5.1",
        "gpt-5",
        "gpt-5-mini",
        "gpt-5-nano",
        "gpt-4.1",
        "gpt-4o",
        "gpt-4.1-mini",
        "gpt-4o-mini",
        "gpt-4.1-nano",
    ]
    available = await _get_available_openai_models()
    if available:
        for name in candidates:
            if name in available:
                return name
    return candidates[0] if candidates else "gpt-4o-mini"


async def _tool_handler(call_context: CallContext, params: FunctionCallParams) -> None:
    tool_map = {
        "transfer_call": transfer_call_func,
        "end_call": end_call_func,
    }
    handler = tool_map.get(params.function_name)
    if not handler:
        await params.result_callback({"error": "unknown_tool"})
        return
    if params.function_name == "transfer_call" and call_context:
        # Treat tool invocation as explicit confirmation to transfer.
        setattr(call_context, "transfer_user_confirmed", True)
    logger.info(
        "Tool invoked CallSid={} function={} args={}",
        getattr(call_context, "call_sid", ""),
        params.function_name,
        dict(params.arguments or {}),
    )
    try:
        result = await handler(call_context, dict(params.arguments or {}))
    except Exception as exc:
        result = {"error": str(exc)}
    await params.result_callback(result)


async def _run_pipecat_session(
    websocket: WebSocket,
    call_context: CallContext,
    stream_sid: str,
    call_sid: str,
) -> None:
    def _safe_sample_rate(value: str, fallback: int) -> int:
        try:
            rate = int(value)
        except Exception:
            rate = fallback
        if rate <= 0:
            return fallback
        # constrain to common telephony/TTS rates
        if rate not in (8000, 16000, 22050, 24000, 44100, 48000):
            return fallback
        return rate

    input_sample_rate = _safe_sample_rate(os.getenv("PIPELINE_INPUT_SAMPLE_RATE", "16000"), 16000)
    output_sample_rate = _safe_sample_rate(os.getenv("PIPELINE_OUTPUT_SAMPLE_RATE", "16000"), 16000)
    twilio_account_sid = os.getenv("TWILIO_ACCOUNT_SID") or None
    twilio_auth_token = os.getenv("TWILIO_AUTH_TOKEN") or None
    twilio_region = os.getenv("TWILIO_REGION") or None
    twilio_edge = os.getenv("TWILIO_EDGE") or None
    serializer_params = TwilioFrameSerializer.InputParams(
        twilio_sample_rate=8000,
        sample_rate=input_sample_rate,
        auto_hang_up=False,
    )
    serializer = TwilioFrameSerializer(
        stream_sid=stream_sid,
        call_sid=call_sid,
        account_sid=twilio_account_sid,
        auth_token=twilio_auth_token,
        region=twilio_region,
        edge=twilio_edge,
        params=serializer_params,
    )

    vad_analyzer = None
    vad_enabled = os.getenv("PIPELINE_VAD_ENABLED", "true").lower() == "true"
    if vad_enabled:
        try:
            from pipecat.audio.vad.silero import SileroVADAnalyzer

            if input_sample_rate in (8000, 16000):
                vad_analyzer = SileroVADAnalyzer(sample_rate=input_sample_rate)
            else:
                logger.warning(
                    "Silero VAD requires 8k/16k input; skipping VAD (rate={}).",
                    input_sample_rate,
                )
        except Exception as exc:
            logger.warning("Silero VAD unavailable; proceeding without VAD: {}", exc)

    transport_params = FastAPIWebsocketParams(
        audio_in_enabled=True,
        audio_out_enabled=True,
        audio_in_sample_rate=input_sample_rate,
        audio_out_sample_rate=output_sample_rate,
        serializer=serializer,
        vad_analyzer=vad_analyzer,
        audio_in_passthrough=True,
    )
    transport = FastAPIWebsocketTransport(websocket, transport_params)

    elevenlabs_key = os.getenv("ELEVENLABS_API_KEY", "")
    if not elevenlabs_key:
        raise RuntimeError("ELEVENLABS_API_KEY is missing")
    stt_language = (
        (os.getenv("STT_FORCE_LANGUAGE", "") or "").strip()
        or (call_context.language or "").strip()
        or os.getenv("DEFAULT_STT_LANGUAGE", "en-US")
    )
    stt = ElevenLabsRealtimeSTTService(
        api_key=elevenlabs_key,
        sample_rate=input_sample_rate,
        language=stt_language,
    )

    tools_schema = _build_tools_schema()
    messages: List[Dict[str, Any]] = []
    prompt_text = call_context.system_message or ""
    if not getattr(call_context, "closing_text", None):
        call_context.closing_text = _extract_closing_from_prompt(prompt_text)
    if not getattr(call_context, "end_call_phrases", None):
        call_context.end_call_phrases = _extract_end_call_phrases_from_prompt(prompt_text)
    system_prompt = _compose_system_prompt(prompt_text)
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    context = LLMContext(messages=messages, tools=tools_schema, tool_choice="auto")

    turn_timeout = float(os.getenv("LLM_USER_TURN_TIMEOUT_SEC", "0.6"))
    user_params = LLMUserAggregatorParams(
        user_turn_stop_timeout=max(0.4, turn_timeout),
    )
    assistant_params = LLMAssistantAggregatorParams()
    user_agg, assistant_agg = LLMContextAggregatorPair(
        context, user_params=user_params, assistant_params=assistant_params
    )
    assistant_context = _AssistantContextTracker(call_context, context)

    env_model = (
        os.getenv("OPENAI_MODEL")
        or os.getenv("LLM_DEFAULT_MODEL")
        or "gpt-4o-mini"
    )
    preferred_model = (call_context.model or "").strip() or env_model
    latency_mode = (getattr(call_context, "latency_mode", "") or "").strip().lower()
    if latency_mode == "turbo":
        preferred_model = os.getenv("LLM_TURBO_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
    model_lower = preferred_model.lower()
    max_tokens = int(os.getenv("LLM_MAX_TOKENS", "160"))
    if latency_mode == "turbo":
        max_tokens = min(max_tokens, int(os.getenv("LLM_TURBO_MAX_TOKENS", "160")))
    if model_lower.startswith("claude-"):
        try:
            from pipecat.services.anthropic.llm import AnthropicLLMService
        except Exception as exc:
            raise RuntimeError(f"Anthropic LLM not available: {exc}") from exc
        temperature = float(os.getenv("LLM_TEMPERATURE", "0.2"))
        anth_params = AnthropicLLMService.InputParams(
            max_tokens=max_tokens,
            temperature=temperature,
        )
        llm_service = AnthropicLLMService(
            model=preferred_model,
            params=anth_params,
            api_key=os.getenv("ANTHROPIC_API_KEY"),
            retry_timeout_secs=float(os.getenv("LLM_RETRY_TIMEOUT_SEC", "1.5")),
            retry_on_timeout=False,
        )
    else:
        model_name = await _resolve_openai_chat_model(preferred_model)
        lower_model = (model_name or "").lower()
        restricted_temp = lower_model.startswith(("o1", "o3", "gpt-5", "gpt-4.1"))
        if restricted_temp:
            temperature = 1.0
        else:
            temperature = float(os.getenv("LLM_TEMPERATURE", "0.2"))
        llm_params = BaseOpenAILLMService.InputParams(
            temperature=temperature,
            max_completion_tokens=max_tokens,
        )
        llm_service = OpenAILLMService(
            model=model_name,
            params=llm_params,
            api_key=os.getenv("OPENAI_API_KEY"),
            retry_timeout_secs=float(os.getenv("LLM_RETRY_TIMEOUT_SEC", "1.5")),
            retry_on_timeout=False,
        )
    for tool in function_tools:
        fn = tool.get("function") or {}
        name = fn.get("name")
        if name:
            llm_service.register_function(name, functools.partial(_tool_handler, call_context))

    fast_agg_enabled = os.getenv("LLM_TEXT_FAST_AGG", "true").lower() == "true"
    if fast_agg_enabled:
        chunk_chars = int(os.getenv("LLM_TEXT_CHUNK_CHARS", "320"))
        min_chars = int(os.getenv("LLM_TEXT_MIN_CHARS", "120"))
        text_aggregator = _FastTextAggregator(chunk_chars=chunk_chars, min_chars=min_chars)
    else:
        text_aggregator = SimpleTextAggregator()
    llm_text = LLMTextProcessor(text_aggregator=text_aggregator)

    tts_voice = (getattr(call_context, "voice", "") or os.getenv("ELEVENLABS_VOICE_ID") or "").strip()
    tts_model = os.getenv("ELEVENLABS_TTS_MODEL", "eleven_turbo_v2_5")
    if not tts_voice:
        raise RuntimeError("ELEVENLABS_VOICE_ID is missing for TTS")
    aggregate_sentences = os.getenv("TTS_AGGREGATE_SENTENCES", "false").lower() == "true"
    tts = StableElevenLabsTTSService(
        api_key=os.getenv("ELEVENLABS_API_KEY"),
        voice_id=tts_voice,
        model=tts_model,
        sample_rate=output_sample_rate,
        aggregate_sentences=aggregate_sentences,
    )

    dedupe_window = float(os.getenv("TRANSCRIPT_DEDUPE_WINDOW_SEC", "3.0"))
    transcript_dedupe = _TranscriptionDeduper(window_sec=max(0.2, dedupe_window))
    noise_filter_enabled = os.getenv("TRANSCRIPT_NOISE_FILTER_ENABLED", "true").lower() == "true"
    echo_filter_enabled = os.getenv("ASSISTANT_ECHO_FILTER_ENABLED", "true").lower() == "true"
    noise_filter = _TranscriptNoiseFilter(
        min_chars=int(os.getenv("TRANSCRIPT_NOISE_MIN_CHARS", "2")),
        min_alpha=int(os.getenv("TRANSCRIPT_NOISE_MIN_ALPHA", "2")),
    )
    echo_filter = _AssistantEchoFilter(call_context)
    debounce_sec = float(os.getenv("USER_UTTERANCE_DEBOUNCE_SEC", "0.05"))

    async def _on_user_text(user_text: str):
        normalized = _normalize_for_match(user_text)
        if not normalized:
            return
        call_context.last_user_text = user_text
        save_call_context(call_context.call_sid, call_context)

    user_logger = _UserTranscriptLogger(call_context, on_user_text=_on_user_text, debounce_sec=debounce_sec)
    tts_tracker = _TTSSpeakingTracker(call_context)
    assistant_output_dedupe = _AssistantOutputDeduper(call_context)
    assistant_safety_guard = _AssistantSafetyGuard(call_context, context)
    assistant_sentence_chunker = _AssistantSentenceChunker()
    assistant_spoken_logger = _AssistantSpokenLogger(call_context)
    one_sentence_enabled = os.getenv("LLM_ONE_SENTENCE_LIMITER", "false").lower() == "true"
    one_sentence_limiter = _OneSentenceLimiter() if one_sentence_enabled else None

    processors = [
        transport.input(),
        stt,
    ]
    turn_detector = _TurnDetector(call_context)
    if noise_filter_enabled:
        processors.append(noise_filter)
    if echo_filter_enabled:
        processors.append(echo_filter)
    processors.append(transcript_dedupe)
    processors.append(turn_detector)
    latency_logger = _LatencyLogger(call_context)
    processors.extend(
        [
            user_logger,
            user_agg,
            llm_service,
            assistant_context,
            llm_text,
            *( [one_sentence_limiter] if one_sentence_limiter else [] ),
            assistant_output_dedupe,
            assistant_safety_guard,
            assistant_sentence_chunker,
            assistant_spoken_logger,
            tts,
            tts_tracker,
            latency_logger,
            transport.output(),
        ]
    )
    pipeline = Pipeline(processors)
    idle_timeout_raw = os.getenv("PIPELINE_IDLE_TIMEOUT_SEC", "0")
    try:
        idle_timeout_val = float(idle_timeout_raw)
    except (TypeError, ValueError):
        idle_timeout_val = 0.0
    idle_timeout_secs = None if idle_timeout_val <= 0 else idle_timeout_val

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            allow_interruptions=True,
            audio_in_sample_rate=input_sample_rate,
            audio_out_sample_rate=output_sample_rate,
        ),
        idle_timeout_secs=idle_timeout_secs,
        cancel_on_idle_timeout=False,
    )
    run_task = asyncio.create_task(task.run(PipelineTaskParams(loop=asyncio.get_running_loop())))

    await run_task


@app.websocket("/connection")
async def websocket_endpoint(websocket: WebSocket):
    requested_header = websocket.headers.get("sec-websocket-protocol") or ""
    requested_subprotocols = [
        proto.strip()
        for proto in requested_header.split(",")
        if proto.strip()
    ]
    preferred_subprotocol = "audio.twilio.com"

    try:
        if any(proto.lower() == preferred_subprotocol for proto in requested_subprotocols):
            await websocket.accept(subprotocol=preferred_subprotocol)
        else:
            await websocket.accept()
    except Exception as exc:
        logger.error("Failed to accept Twilio WebSocket: {}", exc)
        raise

    start_payload = None
    try:
        while True:
            raw = await websocket.receive_text()
            payload = json.loads(raw)
            event = payload.get("event")
            if event == "start":
                start_payload = payload.get("start", {})
                break
            if event == "stop":
                return
    except WebSocketDisconnect:
        return
    except Exception as exc:
        logger.error("Failed to read Twilio start event: {}", exc)
        return

    call_sid = start_payload.get("callSid") or ""
    stream_sid = start_payload.get("streamSid") or ""
    if not stream_sid:
        logger.error("Missing streamSid in Twilio start payload")
        return

    call_context = get_call_context(call_sid) if call_sid else None
    agent_conf = call_agent_mapping.get(call_sid) if call_sid else None
    if not call_context:
        call_context = CallContext()
    call_context.call_sid = call_sid
    call_context.twilio_from = start_payload.get("from") or getattr(call_context, "twilio_from", None)
    call_context.twilio_to = start_payload.get("to") or getattr(call_context, "twilio_to", None)
    if call_context.twilio_to:
        call_context.dialed_number = strip_non_digits(call_context.twilio_to)
    if not agent_conf:
        agent_conf = _select_agent_config(
            agent_id=getattr(call_context, "agent_id", None),
            agent_name=getattr(call_context, "agent_name", None),
            from_number=call_context.twilio_to,
        )
    if agent_conf:
        prompt_text = agent_conf.get("prompt", "") or ""
        if not getattr(call_context, "system_message", None):
            call_context.system_message = prompt_text
        if not getattr(call_context, "intro_text", None):
            intro_text = _extract_intro_from_prompt(prompt_text)
            if not intro_text:
                agent_name = call_context.agent_name or agent_conf.get("name") or "Vikram"
                intro_text = (
                    f"Hello, this is {agent_name} from Godrej Properties. "
                    "Are you currently exploring any investment or home-buying opportunities?"
                )
            call_context.intro_text = intro_text
        if not getattr(call_context, "project_overview_text", None):
            overview_text = _extract_project_overview_from_prompt(prompt_text)
            call_context.project_overview_text = overview_text
        if not getattr(call_context, "closing_text", None):
            closing_text = _extract_closing_from_prompt(prompt_text)
            call_context.closing_text = closing_text
        if not getattr(call_context, "end_call_phrases", None):
            call_context.end_call_phrases = _extract_end_call_phrases_from_prompt(prompt_text)
        if not getattr(call_context, "transfer_number", None):
            call_context.transfer_number = agent_conf.get("transfer_number", "")
        if not getattr(call_context, "voice", None):
            call_context.voice = agent_conf.get("voice", "")
        if not getattr(call_context, "language", None):
            call_context.language = agent_conf.get("language", "en-US")
        if not getattr(call_context, "agent_name", None):
            call_context.agent_name = agent_conf.get("name")
        if getattr(call_context, "human_speaks_first", None) is None:
            call_context.human_speaks_first = agent_conf.get("human_speaks_first", False)
        if not getattr(call_context, "email_tool", None):
            call_context.email_tool = agent_conf.get("email_tool") or os.getenv("EMAIL_TOOL_DEFAULT", "none")
        if not getattr(call_context, "email_recipient", None):
            call_context.email_recipient = agent_conf.get("email_recipient") or os.getenv("EMAIL_RECIPIENT_DEFAULT", "")
        if not getattr(call_context, "agent_type", None):
            call_context.agent_type = agent_conf.get("agent_type")
        if not getattr(call_context, "pricing_data", None):
            call_context.pricing_data = agent_conf.get("pricing_data")
        if not getattr(call_context, "agent_id", None):
            call_context.agent_id = agent_conf.get("id")
        if not getattr(call_context, "latency_mode", None):
            call_context.latency_mode = agent_conf.get("latency_mode")
        # Enable fast intro only when the human is expected to speak first.
        call_context.fast_intro_pending = bool(getattr(call_context, "human_speaks_first", False))

    call_contexts[call_sid] = call_context
    active_stream_contexts[f"{call_sid}_{stream_sid}"] = call_context
    save_call_context(call_sid, call_context)

    try:
        await _run_pipecat_session(websocket, call_context, stream_sid, call_sid)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.error("Pipecat session error: {}", exc)
    finally:
        active_stream_contexts.pop(f"{call_sid}_{stream_sid}", None)

@app.get("/fetch_transcript/{call_sid}")
async def fetch_transcript(call_sid: str):
    """
    Fetch the transcript for a given call_sid from the conversation_logs folder.
    """
    transcript_file_path = os.path.join(LOG_DIR, f"{call_sid}.txt")
    if os.path.exists(transcript_file_path):
        with open(transcript_file_path, "r") as file:
            transcript_content = file.read()
        return {"transcript": transcript_content}
    else:
        return JSONResponse(status_code=404, content={"error": "Transcript not found."})


def _lead_is_interested(summary: Dict[str, Any]) -> bool:
    if summary.get("is_interested"):
        return True
    level = (summary.get("interest_level") or "").lower()
    return level in {"low", "medium", "high"}


def _format_lead_email_body(summary: Dict[str, Any], metadata: Dict[str, Any]) -> str:
    key_points = summary.get("key_points") or []
    key_lines = "\n".join(f"- {point}" for point in key_points) if key_points else "- None"
    interest_reason = summary.get("interest_reason") or ""
    next_steps = summary.get("next_steps") or ""
    lines = [
        "Lead interest detected from call.",
        "",
        f"Agent: {metadata.get('agent_name') or 'Unknown'}",
        f"Lead phone: {metadata.get('lead_phone') or metadata.get('caller_phone') or 'Unknown'}",
        f"Call SID: {metadata.get('call_sid') or 'Unknown'}",
        f"Interest level: {summary.get('interest_level') or 'none'}",
        "",
        "Summary:",
        summary.get("summary") or "No summary available.",
        "",
        "Key points:",
        key_lines,
    ]
    if interest_reason:
        lines.extend(["", "Interest reason:", interest_reason])
    if next_steps:
        lines.extend(["", "Suggested next steps:", next_steps])
    return "\n".join(lines)


def _resolve_lead_phone(call_context: CallContext) -> str:
    agent_type = (getattr(call_context, "agent_type", "") or "").lower()
    if agent_type == "outbound":
        return (
            getattr(call_context, "twilio_to", None)
            or getattr(call_context, "dialed_number", None)
            or getattr(call_context, "twilio_from", None)
            or ""
        )
    if agent_type == "inbound":
        return (
            getattr(call_context, "twilio_from", None)
            or getattr(call_context, "twilio_to", None)
            or getattr(call_context, "dialed_number", None)
            or ""
        )
    app_number = os.getenv("APP_NUMBER", "")
    twilio_from = getattr(call_context, "twilio_from", None)
    twilio_to = getattr(call_context, "twilio_to", None)
    if app_number:
        if twilio_from and app_number in strip_non_digits(twilio_from):
            return twilio_to or getattr(call_context, "dialed_number", None) or twilio_from or ""
        if twilio_to and app_number in strip_non_digits(twilio_to):
            return twilio_from or getattr(call_context, "dialed_number", None) or twilio_to or ""
    return (
        twilio_from
        or twilio_to
        or getattr(call_context, "dialed_number", None)
        or ""
    )


async def maybe_send_lead_email(call_sid: str, transcript_text: str) -> None:
    try:
        call_context = call_contexts.get(call_sid) or get_call_context(call_sid)
        if not call_context:
            logger.warning("Lead email skipped: call context missing for %s.", call_sid)
            return

        email_tool = (getattr(call_context, "email_tool", "") or "").lower()
        if email_tool not in {"lead_email", "enabled", "true", "yes"}:
            logger.info("Lead email disabled for call %s.", call_sid)
            return

        recipient = (
            getattr(call_context, "email_recipient", None)
            or os.getenv("EMAIL_RECIPIENT_DEFAULT", "")
        ).strip()
        if not recipient:
            logger.warning("Lead email skipped: recipient missing for %s.", call_sid)
            return

        lead_phone = _resolve_lead_phone(call_context)
        metadata = {
            "call_sid": call_sid,
            "agent_name": getattr(call_context, "agent_name", None),
            "lead_phone": lead_phone,
            "caller_phone": lead_phone,
        }
        summary = await summarize_lead(transcript_text, metadata)
        if not _lead_is_interested(summary):
            logger.info("Lead email skipped: no interest detected for %s.", call_sid)
            return

        subject_agent = metadata.get("agent_name") or "Agent"
        subject_phone = metadata.get("lead_phone") or "Unknown"
        subject = f"Interested lead - {subject_agent} - {subject_phone}"
        body = _format_lead_email_body(summary, metadata)

        sender = os.getenv("EMAIL_SENDER", "").strip()
        app_password = os.getenv("EMAIL_APP_PASSWORD", "").strip()
        if not sender or not app_password:
            logger.warning("Lead email skipped: sender credentials missing.")
            return

        await asyncio.to_thread(
            send_email,
            subject,
            body,
            recipient,
            from_email=sender,
            app_password=app_password,
        )
    except Exception as exc:
        logger.error("Failed to send lead email for {}: {}", call_sid, exc)






@app.get("/get_twilio_numbers")
async def get_twilio_numbers():
    try:
        client = get_twilio_client()
        incoming_phone_numbers = client.incoming_phone_numbers.list()
        
        # Create a list of all Twilio numbers
        twilio_numbers = [{"phone_number": number.phone_number} for number in incoming_phone_numbers]
        return {"numbers": twilio_numbers}
    except Exception as e:
        logger.error(f"Error fetching Twilio numbers: {str(e)}")
        return {"error": "Failed to fetch Twilio numbers"}
    

@app.get("/calllogs")
async def get_call_logs(
    month: str = None,
    date: str = None,
    phone: str = None,
    page: int = 1,
    page_size: int = 20,
    sip_only: bool = True,
):
    try:
        client = get_twilio_client()
        fetch_limit = int(os.getenv("CALL_LOGS_FETCH_LIMIT", "2000"))
        fetch_limit = min(max(fetch_limit, 100), 10000)
        page = max(1, int(page or 1))
        page_size = min(max(1, int(page_size or 20)), 100)
        phone_query = strip_non_digits(phone or "")
        sip_prefix = (os.getenv("CALL_LOGS_SIP_PREFIX", "sip:") or "sip:").lower()
        sip_host = (
            (os.getenv("CALL_LOGS_SIP_HOST", "") or "").strip()
            or (os.getenv("ASTERISK_SIP_HOST", "") or "").strip()
        ).lower()

        calls = client.calls.list(limit=fetch_limit)

        range_start = None
        range_end = None
        if date:
            day_start = datetime.strptime(date, "%Y-%m-%d")
            range_start = pytz.UTC.localize(day_start)
            range_end = range_start + timedelta(days=1)
        elif month:
            month_start = datetime.strptime(month, "%Y-%m")
            next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
            range_start = pytz.UTC.localize(month_start)
            range_end = pytz.UTC.localize(next_month)

        filtered_calls = []
        for call in calls:
            start_time = getattr(call, "start_time", None)
            to_value = str(getattr(call, "to", "") or "")
            from_value = str(getattr(call, "from_", "") or "")
            to_l = to_value.lower()
            from_l = from_value.lower()

            if sip_only and not (to_l.startswith(sip_prefix) or from_l.startswith(sip_prefix)):
                continue
            if sip_only and sip_host:
                to_host_match = to_l.startswith(sip_prefix) and f"@{sip_host}" in to_l
                from_host_match = from_l.startswith(sip_prefix) and f"@{sip_host}" in from_l
                if not (to_host_match or from_host_match):
                    continue
            if range_start and range_end:
                if not start_time:
                    continue
                call_time = start_time if start_time.tzinfo else pytz.UTC.localize(start_time)
                if not (range_start <= call_time < range_end):
                    continue
            if phone_query:
                if phone_query not in strip_non_digits(to_value) and phone_query not in strip_non_digits(from_value):
                    continue
            filtered_calls.append(call)

        def _call_ts(record):
            ts = getattr(record, "start_time", None)
            if not ts:
                return 0.0
            if ts.tzinfo is None:
                ts = pytz.UTC.localize(ts)
            return ts.timestamp()

        filtered_calls.sort(key=_call_ts, reverse=True)
        total_records = len(filtered_calls)
        total_pages = max(1, (total_records + page_size - 1) // page_size)
        if page > total_pages:
            page = total_pages
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        page_calls = filtered_calls[start_idx:end_idx]

        items = []
        for record in page_calls:
            from_number = record.from_ if getattr(record, "from_", None) else "N/A"
            from_formatted = (
                record.from_formatted
                if getattr(record, "from_formatted", None)
                else from_number
            )
            items.append(
                {
                    "call_sid": record.sid,
                    "from_formatted": from_formatted,
                    "from": from_number,
                    "to": record.to if getattr(record, "to", None) else "N/A",
                    "status": record.status,
                    "duration": record.duration,
                    "start_time": str(record.start_time) if getattr(record, "start_time", None) else "",
                    "end_time": str(record.end_time) if getattr(record, "end_time", None) else "",
                    "cost": record.price if getattr(record, "price", None) else "N/A",
                    "direction": record.direction if getattr(record, "direction", None) else "",
                }
            )

        return {
            "items": items,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1,
            },
            "filters": {
                "month": month or "",
                "date": date or "",
                "phone": phone or "",
                "sip_only": sip_only,
            },
        }
    except Exception as e:
        return {"error": str(e)}
    

async def _warm_bulk_tts(voice_id: str) -> None:
    warmup_enabled = os.getenv("BULK_TTS_WARMUP_ENABLED", "true").lower() == "true"
    if not warmup_enabled:
        return
    text = os.getenv("TTS_PREWARM_TEXT", "Hello")
    if not text:
        return
    tts = None
    try:
        tts = TTSFactory.get_tts_service("elevenlabs", voice_id=voice_id)
        await tts.generate(
            {"partialResponseIndex": None, "partialResponse": text},
            interaction_count=1,
        )
    except Exception as exc:
        logger.warning(f"Bulk TTS warmup skipped due to error: {exc}")
    finally:
        if tts is not None:
            try:
                await tts.disconnect()
            except Exception:
                pass


async def _run_bulk_campaign(
    campaign_id: str,
    contacts: List[Dict[str, str]],
    transfer_number: str,
    twilio_number: Optional[str],
    system_message: Optional[str],
    voice: str,
    language: str,
    email_tool: Optional[str],
    email_recipient: Optional[str],
    agent_id: Optional[str],
    agent_name: Optional[str],
    agent_type: Optional[str],
    model: Optional[str],
    human_speaks_first: Optional[bool],
) -> None:
    sip_enabled = all([
        os.getenv("TWILIO_SIP_DOMAIN"),
        os.getenv("TWILIO_SIP_USERNAME"),
        os.getenv("ASTERISK_SIP_HOST")
    ])
    server = os.getenv("SERVER")
    if not server:
        raise RuntimeError("SERVER env var missing")

    service_url = f"https://{server}/incoming"
    status_callback = f"https://{server}/end_call_status"
    client = get_twilio_client()

    agent_conf = _select_agent_config(agent_id=agent_id, agent_name=agent_name, from_number=twilio_number)
    prefer_agent_defaults = bool(agent_id or agent_name)
    agent_prompt = system_message or agent_conf.get("prompt") or read_global_prompt()
    agent_first_sentence = ""
    agent_type_value = agent_type or agent_conf.get("agent_type") or "outbound"
    human_first_flag = bool(human_speaks_first or agent_conf.get("human_speaks_first"))
    if prefer_agent_defaults:
        voice = agent_conf.get("voice") or voice
        language = agent_conf.get("language") or language
        transfer_number = agent_conf.get("transfer_number") or transfer_number
    email_tool_value = email_tool or agent_conf.get("email_tool") or os.getenv("EMAIL_TOOL_DEFAULT", "none")
    email_recipient_value = email_recipient or agent_conf.get("email_recipient") or os.getenv("EMAIL_RECIPIENT_DEFAULT", "")
    agent_model = str((model or agent_conf.get("model") or "")).strip().lower()

    if not human_first_flag:
        await _warm_bulk_tts(voice)

    max_in_progress = int(os.getenv("BULK_MAX_IN_PROGRESS", "0"))

    for contact in contacts:
        dest = str((contact or {}).get("phone_number") or "").strip()
        contact_name = str((contact or {}).get("name") or "").strip()
        if not dest:
            continue
        upsert_campaign_contact(
            campaign_id,
            dest,
            name=contact_name,
            fields={
                "status": "queued",
                "result": "queued",
                "answered": 0,
                "transferred": 0,
                "updated_at": datetime.utcnow().isoformat(),
            },
        )
        try:
            if max_in_progress > 0:
                while True:
                    snapshot = get_campaign(campaign_id) or {}
                    initiated = int(snapshot.get("initiated", 0))
                    completed = int(snapshot.get("completed", 0))
                    in_progress = max(initiated - completed, 0)
                    if in_progress < max_in_progress:
                        break
                    await asyncio.sleep(1.0)

            if sip_enabled:
                call = await _create_twilio_call(
                    client,
                    {
                        "to": f"sip:{dest}@{os.getenv('ASTERISK_SIP_HOST')}",
                        "from_": os.getenv("TWILIO_SIP_USERNAME"),
                        "url": service_url,
                        "status_callback": status_callback,
                        "status_callback_method": "POST",
                        "timeout": 55,
                    },
                )
            else:
                call = await _create_twilio_call(
                    client,
                    {
                        "to": dest,
                        "from_": twilio_number,
                        "url": service_url,
                        "status_callback": status_callback,
                        "status_callback_method": "POST",
                        "timeout": 55,
                    },
                )

            increment_campaign(campaign_id, "initiated", 1)

            ctx = CallContext()
            ctx.system_message = agent_prompt
            ctx.call_sid = call.sid
            ctx.transfer_number = transfer_number
            ctx.voice = voice
            ctx.language = language
            ctx.agent_name = agent_name or agent_conf.get("name")
            ctx.email_tool = email_tool_value
            ctx.email_recipient = email_recipient_value
            ctx.agent_type = agent_type_value
            ctx.human_speaks_first = human_first_flag
            ctx.twilio_from = twilio_number
            ctx.twilio_to = dest
            ctx.campaign_id = campaign_id
            ctx.lead_name = contact_name
            ctx.created_at = datetime.utcnow().isoformat()
            if agent_model:
                ctx.model = agent_model
            if dest:
                ctx.dialed_number = strip_non_digits(dest)
            ctx.conversation_history = []

            save_call_context(call.sid, ctx)
            call_contexts[call.sid] = ctx
            call_agent_mapping[call.sid] = agent_conf
            link_call_to_contact(campaign_id, call.sid, dest, name=contact_name)
            update_campaign_contact_by_call_sid(
                campaign_id,
                call.sid,
                {
                    "status": "in-progress",
                    "result": "active",
                    "answered": 0,
                    "transferred": 0,
                    "initiated_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat(),
                },
            )

        except Exception as exc:
            logger.error(f"Bulk call failed {dest}: {exc}", exc_info=True)
            increment_campaign(campaign_id, "failed_initiate", 1)
            upsert_campaign_contact(
                campaign_id,
                dest,
                name=contact_name,
                fields={
                    "status": "failed_initiate",
                    "result": "failed_initiate",
                    "error": str(exc),
                    "answered": 0,
                    "transferred": 0,
                    "completed_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat(),
                },
            )

    set_campaign_fields(
        campaign_id,
        {
            "status": "dialing_complete",
            "dialing_completed_at": datetime.utcnow().isoformat(),
        },
    )

@app.post("/start_bulk_calls")
async def start_bulk_calls(
    to_numbers:      List[str] = Body(default=[]),
    contacts:        Optional[List[Dict[str, str]]] = Body(default=None),
    campaign_name:   Optional[str] = Body(default=None),
    transfer_number: str      = Body(...),
    twilio_number:   str      = Body(None),
    system_message:  str      = Body(None),
    voice:           str      = Body(...),
    language:        str      = Body(...),
    email_tool:      Optional[str] = Body(None),
    email_recipient: Optional[str] = Body(None),
    agent_id:        Optional[str] = Body(None),
    agent_name:      Optional[str] = Body(None),
    agent_type:      Optional[str] = Body(None),
    model:           Optional[str] = Body(None),
    human_speaks_first: Optional[bool] = Body(None),
):
    if not contacts and not to_numbers:
        raise HTTPException(400, "Either `contacts` or `to_numbers` must be non-empty")
    campaign_name = str(campaign_name or "").strip()
    if not campaign_name:
        raise HTTPException(400, "`campaign_name` is required")
    # initial_message removed

    SUPPORTED_LANGS = {
        "bg","ca","zh","zh-TW","zh-HK","cs","da","nl","en-US","en-GB","hi",
        "fr","de","ja","ko","es","sv","pt","it","ru","tr","vi","th","pl"
    }
    if language not in SUPPORTED_LANGS:
        raise HTTPException(400, f"Unsupported language: {language}")

    sip_enabled = all([
        os.getenv("TWILIO_SIP_DOMAIN"),
        os.getenv("TWILIO_SIP_USERNAME"),
        os.getenv("ASTERISK_SIP_HOST")
    ])

    if not sip_enabled and not twilio_number:
        raise HTTPException(
            400, "twilio_number required when SIP is not enabled"
        )

    cleaned_contacts: List[Dict[str, str]] = []
    seen_numbers: Set[str] = set()
    if contacts:
        for row in contacts:
            if not isinstance(row, dict):
                continue
            number = str(row.get("phone_number") or row.get("phone") or "").strip()
            if not number or number in seen_numbers:
                continue
            cleaned_contacts.append(
                {
                    "name": str(row.get("name") or "").strip(),
                    "phone_number": number,
                }
            )
            seen_numbers.add(number)
    if to_numbers:
        for number in to_numbers:
            if not number:
                continue
            trimmed = str(number).strip()
            if not trimmed or trimmed in seen_numbers:
                continue
            cleaned_contacts.append({"name": "", "phone_number": trimmed})
            seen_numbers.add(trimmed)

    if not cleaned_contacts:
        raise HTTPException(400, "No valid phone numbers provided.")

    campaign_id = uuid.uuid4().hex
    create_campaign(
        campaign_id,
        len(cleaned_contacts),
        metadata={
            "campaign_name": campaign_name,
            "agent_id": agent_id or "",
            "agent_name": agent_name or "",
            "agent_type": agent_type or "outbound",
        },
    )
    now_iso = datetime.utcnow().isoformat()
    for contact in cleaned_contacts:
        upsert_campaign_contact(
            campaign_id,
            str(contact.get("phone_number") or "").strip(),
            name=str(contact.get("name") or "").strip(),
            fields={
                "status": "queued",
                "result": "queued",
                "answered": 0,
                "transferred": 0,
                "initiated_at": now_iso,
                "updated_at": now_iso,
            },
        )

    task = asyncio.create_task(
        _run_bulk_campaign(
            campaign_id=campaign_id,
            contacts=cleaned_contacts,
            transfer_number=transfer_number,
            twilio_number=twilio_number,
            system_message=system_message,
            voice=voice,
            language=language,
            email_tool=email_tool,
            email_recipient=email_recipient,
            agent_id=agent_id,
            agent_name=agent_name,
            agent_type=agent_type,
            model=model,
            human_speaks_first=human_speaks_first,
        )
    )
    active_campaign_tasks[campaign_id] = task
    task.add_done_callback(lambda t: active_campaign_tasks.pop(campaign_id, None))

    return {
        "campaign_id": campaign_id,
        "campaign_name": campaign_name,
        "status": "running",
        "total_targets": len(cleaned_contacts),
        "initiated": 0,
        "completed": 0,
        "success": 0,
        "declined": 0,
        "failed_initiate": 0,
        "failed_total": 0,
        "answered": 0,
        "transferred": 0,
        "active_calls": 0,
        "in_progress": 0,
        "completion_pct": 0,
    }


@app.get("/campaign_status/{campaign_id}")
async def campaign_status(campaign_id: str = Path(...)):
    snapshot = get_campaign(campaign_id)
    if not snapshot:
        raise HTTPException(status_code=404, detail="Campaign not found")
    total_targets = int(snapshot.get("total_targets", 0))
    initiated = int(snapshot.get("initiated", 0))
    completed = int(snapshot.get("completed", 0))
    declined = int(snapshot.get("declined", 0))
    failed_initiate = int(snapshot.get("failed_initiate", 0))
    answered = int(snapshot.get("answered", 0))
    transferred = int(snapshot.get("transferred", 0))
    remaining = max(total_targets - initiated, 0)
    in_progress = max(initiated - completed, 0)
    failed_total = declined + failed_initiate
    completion_pct = 0.0
    if total_targets > 0:
        completion_pct = round((completed + failed_initiate) / total_targets * 100, 2)
    snapshot.update(
        {
            "remaining": remaining,
            "in_progress": in_progress,
            "active_calls": in_progress,
            "failed_total": failed_total,
            "answered": answered,
            "transferred": transferred,
            "completion_pct": completion_pct,
        }
    )
    return snapshot


@app.get("/campaigns_status")
async def campaigns_status(
    q: str = Query("", description="Search by campaign id/name/agent/status"),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=200),
):
    campaigns = list_campaigns(limit=0, offset=0)
    search = (q or "").strip().lower()
    if search:
        filtered = []
        for snapshot in campaigns:
            haystacks = [
                str(snapshot.get("id", "") or ""),
                str(snapshot.get("campaign_name", "") or ""),
                str(snapshot.get("agent_name", "") or ""),
                str(snapshot.get("status", "") or ""),
            ]
            if any(search in value.lower() for value in haystacks):
                filtered.append(snapshot)
        campaigns = filtered

    total_records = len(campaigns)
    total_pages = max(1, (total_records + page_size - 1) // page_size)
    if page > total_pages:
        page = total_pages
    start = (page - 1) * page_size
    end = start + page_size
    campaigns = campaigns[start:end]
    items = []
    for snapshot in campaigns:
        total_targets = int(snapshot.get("total_targets", 0))
        initiated = int(snapshot.get("initiated", 0))
        completed = int(snapshot.get("completed", 0))
        declined = int(snapshot.get("declined", 0))
        failed_initiate = int(snapshot.get("failed_initiate", 0))
        answered = int(snapshot.get("answered", 0))
        transferred = int(snapshot.get("transferred", 0))
        remaining = max(total_targets - initiated, 0)
        in_progress = max(initiated - completed, 0)
        failed_total = declined + failed_initiate
        completion_pct = 0.0
        if total_targets > 0:
            completion_pct = round((completed + failed_initiate) / total_targets * 100, 2)
        snapshot.update(
            {
                "remaining": remaining,
                "in_progress": in_progress,
                "active_calls": in_progress,
                "failed_total": failed_total,
                "answered": answered,
                "transferred": transferred,
                "completion_pct": completion_pct,
            }
        )
        items.append(snapshot)
    return {
        "items": items,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_records": total_records,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_prev": page > 1,
        },
        "filters": {"q": q or ""},
    }


@app.get("/campaign_results/{campaign_id}")
async def campaign_results(
    campaign_id: str = Path(...),
    q: str = Query("", description="Search by name/phone/result/status"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=5000),
):
    snapshot = get_campaign(campaign_id)
    if not snapshot:
        raise HTTPException(status_code=404, detail="Campaign not found")
    contacts = list_campaign_contacts(campaign_id, limit=0, offset=0)
    if not contacts:
        contacts = _fallback_campaign_contacts_from_context(campaign_id)
    for item in contacts:
        item["result"] = _effective_contact_result(item)
    all_contacts = list(contacts)

    search = (q or "").strip().lower()
    if search:
        filtered: List[Dict[str, Any]] = []
        for item in contacts:
            haystacks = [
                str(item.get("name", "") or ""),
                str(item.get("phone_number", "") or ""),
                str(item.get("result", "") or ""),
                str(item.get("status", "") or ""),
                str(item.get("call_sid", "") or ""),
            ]
            if any(search in value.lower() for value in haystacks):
                filtered.append(item)
        contacts = filtered

    analytics = {
        "total": len(all_contacts),
        "answered": sum(1 for item in all_contacts if _coerce_bool_flag(item.get("answered"))),
        "transferred": sum(1 for item in all_contacts if _coerce_bool_flag(item.get("transferred"))),
        "failed": sum(1 for item in all_contacts if str(item.get("result", "")).startswith("failed")),
        "active": sum(1 for item in all_contacts if str(item.get("result", "")) in {"active", "queued"}),
    }

    total_records = len(contacts)
    total_pages = max(1, (total_records + page_size - 1) // page_size)
    if page > total_pages:
        page = total_pages
    start = (page - 1) * page_size
    end = start + page_size
    items = contacts[start:end]
    return {
        "campaign_id": campaign_id,
        "campaign_name": snapshot.get("campaign_name", ""),
        "status": snapshot.get("status", ""),
        "analytics": analytics,
        "total_records": total_records,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_records": total_records,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_prev": page > 1,
        },
        "filters": {"q": q or ""},
        "items": items,
    }


@app.get("/campaign_results_export/{campaign_id}")
async def campaign_results_export(
    campaign_id: str = Path(...),
    q: str = Query("", description="Optional search filter"),
):
    snapshot = get_campaign(campaign_id)
    if not snapshot:
        raise HTTPException(status_code=404, detail="Campaign not found")
    contacts = list_campaign_contacts(campaign_id, limit=0, offset=0)
    if not contacts:
        contacts = _fallback_campaign_contacts_from_context(campaign_id)
    search = (q or "").strip().lower()
    if search:
        filtered: List[Dict[str, Any]] = []
        for item in contacts:
            effective_result = _effective_contact_result(item)
            haystacks = [
                str(item.get("name", "") or ""),
                str(item.get("phone_number", "") or ""),
                str(item.get("status", "") or ""),
                str(effective_result),
            ]
            if any(search in value.lower() for value in haystacks):
                filtered.append(item)
        contacts = filtered
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "campaign_id",
            "campaign_name",
            "name",
            "phone_number",
            "call_sid",
            "status",
            "result",
            "answered",
            "transferred",
            "duration_sec",
            "initiated_at",
            "completed_at",
        ]
    )
    for item in contacts:
        writer.writerow(
            [
                campaign_id,
                snapshot.get("campaign_name", ""),
                item.get("name", ""),
                item.get("phone_number", ""),
                item.get("call_sid", ""),
                item.get("status", ""),
                _effective_contact_result(item),
                1 if _coerce_bool_flag(item.get("answered")) else 0,
                1 if _coerce_bool_flag(item.get("transferred")) else 0,
                item.get("duration_sec", 0),
                item.get("initiated_at", ""),
                item.get("completed_at", ""),
            ]
        )
    filename = f"campaign_{campaign_id[:8]}_results_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers=headers)



@app.get("/dashboard_stats")
async def dashboard_stats(
    month: str = None,
    date: str = None,
    phone: str = None,
    direction: str = "all",
    sip_only: bool = True,
):
    try:
        client = get_twilio_client()
        fetch_limit = int(os.getenv("DASHBOARD_FETCH_LIMIT", os.getenv("CALL_LOGS_FETCH_LIMIT", "2000")))
        fetch_limit = min(max(fetch_limit, 100), 10000)
        calls = client.calls.list(limit=fetch_limit)

        # Validate filters
        range_start = None
        range_end = None
        if date:
            try:
                day_start = datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=422, detail="Invalid date format. Use YYYY-MM-DD")
            range_start = pytz.UTC.localize(day_start)
            range_end = range_start + timedelta(days=1)
        elif month:
            try:
                month_start = datetime.strptime(month, "%Y-%m")
            except ValueError:
                raise HTTPException(status_code=422, detail="Invalid month format. Use YYYY-MM")
            next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
            range_start = pytz.UTC.localize(month_start)
            range_end = pytz.UTC.localize(next_month)

        direction_norm = (direction or "all").strip().lower()
        allowed_directions = {"all", "inbound", "outbound-api"}
        if direction_norm not in allowed_directions:
            raise HTTPException(status_code=422, detail="Invalid direction. Use all, inbound, or outbound-api")

        phone_query = strip_non_digits(phone or "")
        sip_prefix = (os.getenv("CALL_LOGS_SIP_PREFIX", "sip:") or "sip:").lower()
        sip_host = (
            (os.getenv("CALL_LOGS_SIP_HOST", "") or "").strip()
            or (os.getenv("ASTERISK_SIP_HOST", "") or "").strip()
        ).lower()

        filtered_calls = []
        for call in calls:
            start_time = getattr(call, "start_time", None)
            call_direction = (getattr(call, "direction", "") or "").lower()
            to_value = str(getattr(call, "to", "") or "")
            from_value = str(getattr(call, "from_", "") or "")
            to_l = to_value.lower()
            from_l = from_value.lower()

            if direction_norm != "all" and call_direction != direction_norm:
                continue
            if sip_only and not (to_l.startswith(sip_prefix) or from_l.startswith(sip_prefix)):
                continue
            if sip_only and sip_host:
                to_host_match = to_l.startswith(sip_prefix) and f"@{sip_host}" in to_l
                from_host_match = from_l.startswith(sip_prefix) and f"@{sip_host}" in from_l
                if not (to_host_match or from_host_match):
                    continue
            if range_start and range_end:
                if not start_time:
                    continue
                call_time = start_time if start_time.tzinfo else pytz.UTC.localize(start_time)
                if not (range_start <= call_time < range_end):
                    continue
            if phone_query:
                if phone_query not in strip_non_digits(to_value) and phone_query not in strip_non_digits(from_value):
                    continue
            filtered_calls.append(call)

        total_calls = len(filtered_calls)
        inbound_calls = sum(1 for c in filtered_calls if (getattr(c, "direction", "") or "").lower() == "inbound")
        outbound_calls = sum(1 for c in filtered_calls if (getattr(c, "direction", "") or "").lower() == "outbound-api")

        total_duration_sec = 0
        for call in filtered_calls:
            try:
                total_duration_sec += int(getattr(call, "duration", 0) or 0)
            except (TypeError, ValueError):
                continue
        average_duration = round((total_duration_sec / total_calls) / 60, 2) if total_calls else 0

        # Daily trend data sorted by actual date for reliable charting.
        daily_counts: Dict[str, int] = {}
        for call in filtered_calls:
            start_time = getattr(call, "start_time", None)
            if not start_time:
                continue
            call_time = start_time if start_time.tzinfo else pytz.UTC.localize(start_time)
            key = call_time.strftime("%Y-%m-%d")
            daily_counts[key] = daily_counts.get(key, 0) + 1

        trend_list = [{"day": day, "count": daily_counts[day]} for day in sorted(daily_counts.keys())]

        return {
            "total_calls": total_calls,
            "inbound_calls": inbound_calls,
            "outbound_calls": outbound_calls,
            "average_duration": average_duration,
            "daily_trend": trend_list,
            "filters": {
                "month": month or "",
                "date": date or "",
                "phone": phone or "",
                "direction": direction_norm,
                "sip_only": sip_only,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        return {"error": str(e)}






@app.get("/calling_stats")
async def get_calling_stats(month: str = None):
    try:
        client = get_twilio_client()
        calls = client.calls.list(limit=1000)  # Fetch more logs if needed

        # Set default time range
        if not month:
            today = datetime.utcnow().date()
            start_date = datetime.combine(today, datetime.min.time()).replace(tzinfo=pytz.UTC)
            end_date = datetime.combine(today, datetime.max.time()).replace(tzinfo=pytz.UTC)
        else:
            start_date = datetime.strptime(month, "%Y-%m")
            end_date = (start_date.replace(day=28) + timedelta(days=4)).replace(day=1)
            start_date = pytz.UTC.localize(start_date)
            end_date = pytz.UTC.localize(end_date)

        filtered_calls = [call for call in calls if start_date <= call.start_time <= end_date]

        total_calls = len(filtered_calls)
        total_duration_sec = sum(int(call.duration or 0) for call in filtered_calls)
        total_duration_min = round(total_duration_sec / 60, 2)
        avg_duration = round(total_duration_sec / total_calls / 60, 2) if total_calls > 0 else 0
        total_cost = round(total_duration_min * 0.12, 2)

        inbound_calls = sum(1 for call in filtered_calls if call.direction == "inbound")
        outbound_calls = total_calls - inbound_calls

        # For charting: daily call count
        daily_counts = {}
        for call in filtered_calls:
            date_str = call.start_time.strftime("%Y-%m-%d")
            daily_counts[date_str] = daily_counts.get(date_str, 0) + 1

        return {
            "total_calls": total_calls,
            "inbound_calls": inbound_calls,
            "outbound_calls": outbound_calls,
            "avg_duration": avg_duration,
            "total_duration": total_duration_min,
            "total_cost": total_cost,
            "daily_counts": [{"date": k, "count": v} for k, v in sorted(daily_counts.items())]
        }

    except Exception as e:
        return {"error": str(e)}




# Replace with your actual ElevenLabs API key
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")

@app.get("/get_elevenlabs_voices")
async def get_elevenlabs_voices():
    url = "https://api.elevenlabs.io/v1/voices"
    headers = {
        "Content-Type": "application/json",
        "xi-api-key": ELEVENLABS_API_KEY,
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            voices = response.json().get("voices", [])
            return {"voices": voices}
        else:
            return {"error": f"Failed to fetch voices: {response.text}"}
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}

@app.get("/generate_summary/{call_sid}")
async def generate_summary(call_sid: str):
    """
    Generate a summary for the transcript using the call_sid.
    """
    transcript_file = os.path.join(LOG_DIR, f"{call_sid}.txt")

    # Check if the file exists
    if not os.path.exists(transcript_file):
        return {"error": f"Transcript file not found for call_sid: {call_sid}"}

    try:
        with open(transcript_file, "r") as file:
            transcript_text = file.read()

        # Use OpenAI to generate the summary
        prompt = f"Summarize the following conversation transcript in exactly 2 lines:\n\n{transcript_text}"

        model_name = os.getenv("OPENAI_MODEL", "gpt-5-mini")
        lower_model = (model_name or "").lower()
        restricted_temp = lower_model.startswith(("o1", "o3", "gpt-5", "gpt-4.1"))
        token_param = "max_completion_tokens" if restricted_temp else "max_tokens"
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "You are a summarization assistant."},
                {"role": "user", "content": prompt},
            ],
            **({} if restricted_temp else {"temperature": 0.5}),
            **{token_param: 100},
        )

        summary = (response.choices[0].message.content or "").strip()
        return {"summary": summary}

    except Exception as e:
        return {"error": f"Failed to generate summary: {str(e)}"}

MOCK_API_URL = "https://6912bf3b52a60f10c8228b5b.mockapi.io/associatteai"
PROMPT_CACHE_FILE = os.path.join(os.getcwd(), "prompt_cache.json")

def _default_agent():
    return {
        "name": "Default Agent",
        "prompt": read_global_prompt(),
        "from_number": os.getenv("APP_NUMBER", ""),
        "transfer_number": os.getenv("TRANSFER_NUMBER", ""),
        "voice": os.getenv("ELEVENLABS_VOICE_ID", ""),
        "language": "en-US",
        "agent_type": "outbound",
        "email_tool": os.getenv("EMAIL_TOOL_DEFAULT", "none"),
        "email_recipient": os.getenv("EMAIL_RECIPIENT_DEFAULT", ""),
    }


def _load_cached_prompts():
    if os.path.exists(PROMPT_CACHE_FILE):
        try:
            with open(PROMPT_CACHE_FILE, "r") as cache_file:
                data = json.load(cache_file)
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            item.pop("first_sentence", None)
                            item.pop("INITIAL_MESSAGE", None)
                    return data
        except Exception as exc:
            logger.error(f"Error reading prompts cache: {exc}")
    return []

def load_agent_configs():
    try:
        response = requests.get(MOCK_API_URL, timeout=8)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            raise ValueError("Agent API returned unexpected payload")
        # Strip deprecated greeting fields from remote payloads
        for item in data:
            if isinstance(item, dict):
                item.pop("first_sentence", None)
                item.pop("INITIAL_MESSAGE", None)
        with open(PROMPT_CACHE_FILE, "w") as cache_file:
            json.dump(data, cache_file)
        return data
    except Exception as exc:
        logger.warning(f"Failed to fetch prompts from remote API: {exc}")
        cached = _load_cached_prompts()
        if cached:
            logger.info("Using cached prompts.")
            return cached
        local_agents = load_local_agent_configs()
        if local_agents:
            logger.info("Using local inbound_config fallback for prompts.")
            return local_agents
        logger.info("Using default agent fallback.")
        return [_default_agent()]

@app.get("/fetch_prompts")
async def fetch_prompts():
    data = load_agent_configs()
    return JSONResponse(content=data, status_code=200)

from twilio.base.exceptions import TwilioRestException

@app.post("/add_prompt")
async def add_prompt(data: dict = Body(...)):
    # ✅ Set predefined from_number
    predefined_from_number = "sip:+923463952555@147.79.67.103"

    # Validate required fields
    required_fields = ["name", "prompt", "language", "voice", "transfer_number", "agent_type", "model"]
    missing_fields = [field for field in required_fields if field not in data or not data[field]]
    if missing_fields:
        raise HTTPException(status_code=400, detail=f"Missing required fields: {', '.join(missing_fields)}")

    # ✅ Override or inject predefined number
    data["from_number"] = predefined_from_number
    data.pop("first_sentence", None)
    data.pop("INITIAL_MESSAGE", None)
    data.setdefault("email_tool", os.getenv("EMAIL_TOOL_DEFAULT", "none"))
    if not data.get("email_recipient"):
        default_recipient = os.getenv("EMAIL_RECIPIENT_DEFAULT", "")
        if default_recipient:
            data["email_recipient"] = default_recipient

    # ✅ Fix Twilio compatibility (remove SIP format before calling API)
    twilio_number = predefined_from_number
    if twilio_number.startswith("sip:"):
        twilio_number = twilio_number.split('@')[0].replace("sip:", "")

    try:
        # Add the prompt via the mock API
        response = requests.post(MOCK_API_URL, json=data)
        if response.status_code not in [200, 201]:
            return JSONResponse(
                content={"error": f"Failed to add prompt: {response.text}"},
                status_code=response.status_code
            )

        response_data = response.json()
        agent_id = response_data.get("id")
        if not agent_id:
            raise ValueError("Agent ID not returned from mock API")

        # Configure Twilio webhook for the number
        try:
            client = get_twilio_client()
            phone_numbers = client.incoming_phone_numbers.list(phone_number=twilio_number)

            if not phone_numbers:
                raise ValueError(f"No Twilio phone number found for {twilio_number}")

            phone_number_sid = phone_numbers[0].sid
            server = os.getenv("SERVER")
            if not server:
                raise HTTPException(status_code=500, detail="SERVER environment variable not set")

            webhook_url = f"https://{server}/incoming"

            client.incoming_phone_numbers(phone_number_sid).update(
                voice_url=webhook_url,
                voice_method="POST"
            )

            return JSONResponse(
                content={
                    "message": "Prompt added and Twilio webhook configured successfully",
                    "agent_id": agent_id,
                    "from_number": predefined_from_number
                },
                status_code=201
            )

        except TwilioRestException as twilio_err:
            logger.error(f"Twilio error setting webhook: {twilio_err}")
            return JSONResponse(
                content={
                    "message": "Prompt added successfully, but failed to configure Twilio webhook",
                    "twilio_error": str(twilio_err),
                    "from_number": predefined_from_number
                },
                status_code=201
            )

        except ValueError as ve:
            logger.error(f"Error setting webhook: {ve}")
            return JSONResponse(
                content={
                    "message": "Prompt added successfully, but failed to configure Twilio webhook",
                    "error": str(ve),
                    "from_number": predefined_from_number
                },
                status_code=201
            )

    except Exception as e:
        logger.error(f"Error adding prompt: {e}")
        raise HTTPException(status_code=500, detail=f"Error adding prompt: {str(e)}")


@app.delete("/delete_prompt/{prompt_id}")
async def delete_prompt(prompt_id: int):
    response = requests.delete(f"{MOCK_API_URL}/{prompt_id}")
    if response.status_code == 200:
        return JSONResponse(content={"message": "Prompt deleted successfully"}, status_code=200)
    else:
        return JSONResponse(content={"error": "Failed to delete prompt"}, status_code=400)

@app.put("/update_prompt/{prompt_id}")
async def update_prompt(prompt_id: str = Path(...), data: dict = Body(...)):
    try:
        # MockAPI PUT semantics can overwrite missing keys; merge to support partial updates.
        existing: Dict[str, Any] = {}
        try:
            existing_resp = requests.get(f"{MOCK_API_URL}/{prompt_id}", timeout=8)
            if existing_resp.status_code == 200:
                existing_payload = existing_resp.json()
                if isinstance(existing_payload, dict):
                    existing = existing_payload
        except Exception:
            existing = {}

        merged = dict(existing)
        merged.update(data or {})
        response = requests.put(f"{MOCK_API_URL}/{prompt_id}", json=merged)
        if response.status_code in [200, 201]:
            return JSONResponse(content={"message": "Prompt updated successfully"}, status_code=200)
        else:
            return JSONResponse(content={"error": f"Failed to update prompt: {response.text}"}, status_code=500)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)



# File path for inbound settings JSON
INBOUND_CONFIG_FILE = os.getenv(
    "INBOUND_CONFIG_FILE",
    str(BASE_DIR / "inbound_config.json"),
)


def read_inbound_config():
    """Return the raw inbound configuration (dict or list)."""
    if os.path.exists(INBOUND_CONFIG_FILE):
        try:
            with open(INBOUND_CONFIG_FILE, "r") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid inbound_config.json: {e}")
        except OSError as exc:
            logger.error(f"Unable to read inbound_config.json: {exc}")
    return {}


def _normalize_agent_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Map arbitrary inbound config records to the fields used by the dialer."""
    if not isinstance(record, dict):
        return _default_agent()

    prompt = record.get("prompt") or record.get("SYSTEM_MESSAGE") or read_global_prompt()
    from_number = record.get("from_number") or record.get("TWILIO_NUMBER") or os.getenv("APP_NUMBER", "")
    transfer_number = record.get("transfer_number") or record.get("TRANSFER_NUMBER") or os.getenv("TRANSFER_NUMBER", "")
    voice = record.get("voice") or record.get("ELEVENLABS_VOICE_ID") or os.getenv("ELEVENLABS_VOICE_ID", "")
    language = record.get("language") or os.getenv("DEFAULT_AGENT_LANGUAGE", "en-US")
    agent_name = record.get("name") or record.get("agent_name") or "Inbound Agent"
    email_tool = record.get("email_tool") or record.get("EMAIL_TOOL") or os.getenv("EMAIL_TOOL_DEFAULT", "none")
    email_recipient = record.get("email_recipient") or record.get("EMAIL_RECIPIENT") or os.getenv("EMAIL_RECIPIENT_DEFAULT", "")
    normalized = {
        "name": agent_name,
        "prompt": prompt,
        "from_number": from_number,
        "TWILIO_NUMBER": record.get("TWILIO_NUMBER") or from_number,
        "transfer_number": transfer_number,
        "voice": voice,
        "language": language,
        "agent_type": record.get("agent_type") or "inbound",
        "id": record.get("id"),
        "email_tool": email_tool,
        "email_recipient": email_recipient,
    }
    return normalized


def load_local_agent_configs() -> List[Dict[str, Any]]:
    """Load agent definitions from disk/env so inbound calls never fail."""
    configs: List[Dict[str, Any]] = []
    raw = read_inbound_config()

    if isinstance(raw, list):
        for item in raw:
            normalized = _normalize_agent_record(item)
            if normalized:
                configs.append(normalized)
    elif isinstance(raw, dict) and raw:
        configs.append(_normalize_agent_record(raw))

    if not configs:
        configs.append(_default_agent())
    return configs


def read_inbound_settings_data() -> Dict[str, Any]:
    """Return settings payload expected by the UI."""
    raw = read_inbound_config()
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list) and raw:
        first = next((item for item in raw if isinstance(item, dict)), {})
        if not first:
            return {}
        return {
            "SYSTEM_MESSAGE": first.get("prompt") or first.get("SYSTEM_MESSAGE", "") or read_global_prompt(),
            "ELEVENLABS_VOICE_ID": first.get("voice") or first.get("ELEVENLABS_VOICE_ID", ""),
            "language": first.get("language", "en-US"),
            "TRANSFER_NUMBER": first.get("transfer_number") or first.get("TRANSFER_NUMBER", ""),
            "TWILIO_NUMBER": first.get("from_number") or first.get("TWILIO_NUMBER") or "",
        }
    return {}

def write_inbound_config(data):
    try:
        with open(INBOUND_CONFIG_FILE, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to write inbound config: {e}")


@app.get("/agent/{twilio_number}")
async def get_agent_by_twilio(twilio_number: str):
    agents = load_local_agent_configs()
    for agent in agents:
        if agent.get("TWILIO_NUMBER") == twilio_number or agent.get("from_number") == twilio_number:
            return {"agent": agent}
    return {"agent": None}  # No matching agent found


# Pydantic model for inbound settings
class InboundSettings(BaseModel):
    SYSTEM_MESSAGE: Optional[str] = ""
    ELEVENLABS_VOICE_ID: Optional[str] = ""
    language: Optional[str] = ""
    TRANSFER_NUMBER: Optional[str] = ""


class AgentSettings(BaseModel):
    TWILIO_NUMBER: str
    SYSTEM_MESSAGE: str = ""
    ELEVENLABS_VOICE_ID: str = ""
    language: str = ""
    TRANSFER_NUMBER: str = ""
    HUMAN_SPEAKS_FIRST: Optional[bool] = False
    email_tool: Optional[str] = None
    email_recipient: Optional[str] = None
    model: Optional[str] = None
    agent_type: Literal["inbound", "outbound"]

# GET endpoint to return current inbound settings
@app.get("/get_inbound_settings")
async def get_inbound_settings():
    config = read_inbound_settings_data()
    return config

# POST endpoint to update inbound settings
@app.post("/update_inbound_settings")
async def update_inbound_settings(settings: InboundSettings):
    # Read the current settings (if any)
    config = read_inbound_settings_data()
    # Ensure the config is a dictionary; if not, reinitialize it.
    if not isinstance(config, dict):
        config = {}
    # Update with new values from the request
    config.update(settings.dict())
    write_inbound_config(config)
    return {"success": True}


@app.post("/update_agent")
async def update_agent(settings: AgentSettings):
    """Create or update an agent in the mock API and update Twilio webhook URL."""
    server = os.getenv("SERVER")
    if not server:
        raise HTTPException(status_code=500, detail="SERVER environment variable not set")
    
    voice_url = f"https://{server}/incoming"
    agent_data = settings.dict()

    try:
        # Fetch existing agents from the mock API
        response = requests.get(MOCK_API_URL)
        if response.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to fetch existing agents")

        agents = response.json()
        updated = False

        # Check if agent with TWILIO_NUMBER exists
        for agent in agents:
            if agent.get("TWILIO_NUMBER") == settings.TWILIO_NUMBER:
                # Update existing agent
                agent_id = agent.get("id")
                update_response = requests.put(f"{MOCK_API_URL}/{agent_id}", json=agent_data)
                if update_response.status_code not in [200, 201]:
                    raise HTTPException(status_code=500, detail=f"Failed to update agent: {update_response.text}")
                updated = True
                break

        # If no matching agent, create a new one
        if not updated:
            create_response = requests.post(MOCK_API_URL, json=agent_data)
            if create_response.status_code not in [200, 201]:
                raise HTTPException(status_code=500, detail=f"Failed to create agent: {create_response.text}")

        # Update Twilio webhook URL
        if not update_twilio_number_voice_url(settings.TWILIO_NUMBER, voice_url):
            raise HTTPException(status_code=500, detail=f"Failed to update voice URL for {settings.TWILIO_NUMBER}")

        return JSONResponse(content={"success": True, "agent": agent_data}, status_code=200)

    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error communicating with mock API: {str(e)}")


def update_twilio_number_voice_url(phone_number: str, voice_url: str) -> bool:
    """Update the Twilio phone number's voice URL."""
    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    client = Client(account_sid, auth_token)
    
    incoming_numbers = client.incoming_phone_numbers.list(phone_number=phone_number)
    if incoming_numbers:
        incoming_numbers[0].update(voice_url=voice_url, voice_method="POST")
        return True
    return False


# Path to your JSON file
JSON_PATH = os.getenv("TWILIO_MESSAGE_JSON_PATH", str(BASE_DIR / "twilio_message.json"))

@app.get("/get_twilio_message")
async def get_twilio_message():
    """
    Reads the JSON file (or returns an empty message if not present)
    and returns: { "message": "…your SMS template…" }
    """
    try:
        with open(JSON_PATH, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {"message": ""}
    return {"message": data.get("message", "")}


class MessagePayload(BaseModel):
    message: str = ""


@app.post("/update_twilio_message")
async def update_twilio_message(payload: MessagePayload):
    """
    Takes a JSON body like { "message": "new template" },
    writes it to your file, and returns { "success": true }.
    """
    # ensure the directory exists
    os.makedirs(os.path.dirname(JSON_PATH), exist_ok=True)

    try:
        with open(JSON_PATH, "w") as f:
            json.dump({"message": payload.message}, f, indent=2)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to write file: {e}")

    return {"success": True}

if __name__ == "__main__":
    print('in main')
    import uvicorn
    logger.info("Starting server...")
    logger.info(f"Backend server address set to: {os.getenv('SERVER')}")
    port = int(os.getenv("PORT", 3010))
    uvicorn.run(app, host="0.0.0.0", port=port)
