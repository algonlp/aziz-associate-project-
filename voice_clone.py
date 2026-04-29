import os
import logging
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from elevenlabs import ElevenLabs

router = APIRouter()
logging.basicConfig(level=logging.INFO)

VOICE_CLONE_FOLDER = os.path.join(os.getcwd(), "voice_clones")
os.makedirs(VOICE_CLONE_FOLDER, exist_ok=True)


def _get_client() -> ElevenLabs:
    api_key = os.getenv("ELEVENLABS_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=500, detail="ELEVENLABS_API_KEY is not configured")
    return ElevenLabs(api_key=api_key, base_url="https://api.elevenlabs.io")

@router.post("/clone")
async def clone_voice(file: UploadFile = File(...), name: str = Form("Unnamed Voice")):
    try:
        client = _get_client()
        file_path = os.path.join(VOICE_CLONE_FOLDER, file.filename)
        file_bytes = await file.read()

        if not file_bytes:
            raise HTTPException(status_code=400, detail="Uploaded file is empty or corrupted")

        with open(file_path, "wb") as f:
            f.write(file_bytes)

        logging.info(f"✅ File saved at: {file_path}")

        with open(file_path, "rb") as audio_file:
            voice = client.voices.ivc.create(name=name, files=[audio_file])

        voice_id = getattr(voice, "voice_id", None)
        logging.info(f"✅ Voice cloned successfully: {voice_id}")

        return {
            "success": True,
            "message": "Voice cloned successfully! ok",
            "voice_id": voice_id,
            "voice_name": name,
            "saved_path": file_path
        }

    except Exception as e:
        logging.exception("Voice cloning failed")
        raise HTTPException(status_code=500, detail=f"Voice cloning failed: {str(e)}")
