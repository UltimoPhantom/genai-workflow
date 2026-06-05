"""
TTS microservice wrapping Piper TTS.
Accepts {speaker, text} → returns WAV audio bytes.

Each speaker name maps to a Piper voice model. Unknown speakers fall back
to the default voice. Models are downloaded once at startup and cached in
/models inside the container (mounted as a volume).
"""

import hashlib
import logging
import os
import subprocess
import tempfile
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tts-service")

app = FastAPI(title="Piper TTS Service")

MODELS_DIR = Path(os.environ.get("MODELS_DIR", "/models"))
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# Piper voice model registry — maps speaker name → (model_url, config_url)
# Using en_US-lessac-medium as default (good quality, small size, ARM-compatible)
PIPER_BASE = "https://huggingface.co/rhasspy/piper-voices/resolve/main"

VOICE_REGISTRY = {
    "default": ("en_US-lessac-medium", "en/en_US/lessac/medium"),
    "ALICE":   ("en_US-lessac-medium", "en/en_US/lessac/medium"),
    "BOB":     ("en_US-ryan-medium",   "en/en_US/ryan/medium"),
    "NARRATOR":("en_US-lessac-medium", "en/en_US/lessac/medium"),
    "CAPTAIN": ("en_US-ryan-medium",   "en/en_US/ryan/medium"),
    "OFFICER": ("en_US-lessac-medium", "en/en_US/lessac/medium"),
    "ENGINEER":("en_US-ryan-medium",   "en/en_US/ryan/medium"),
    "PILOT":   ("en_US-lessac-medium", "en/en_US/lessac/medium"),
}


def _model_files(voice_name: str, voice_path: str) -> tuple[Path, Path]:
    model_file  = MODELS_DIR / f"{voice_name}.onnx"
    config_file = MODELS_DIR / f"{voice_name}.onnx.json"
    return model_file, config_file


def _ensure_model(voice_name: str, voice_path: str):
    model_file, config_file = _model_files(voice_name, voice_path)
    if model_file.exists() and config_file.exists():
        return

    log.info("Downloading voice model: %s", voice_name)
    base_url = f"{PIPER_BASE}/{voice_path}"

    for url, dest in [
        (f"{base_url}/{voice_name}.onnx",      model_file),
        (f"{base_url}/{voice_name}.onnx.json", config_file),
    ]:
        log.info("Fetching %s", url)
        with httpx.stream("GET", url, follow_redirects=True, timeout=120) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_bytes():
                    f.write(chunk)

    log.info("Model ready: %s", voice_name)


@app.on_event("startup")
async def startup():
    # Pre-download all voices so first request isn't slow
    for speaker, (voice_name, voice_path) in VOICE_REGISTRY.items():
        try:
            _ensure_model(voice_name, voice_path)
        except Exception as e:
            log.warning("Could not pre-download voice %s: %s", voice_name, e)


class TTSRequest(BaseModel):
    speaker: str
    text: str


@app.post("/synthesise")
def synthesise(req: TTSRequest):
    speaker = req.speaker.upper()
    voice_name, voice_path = VOICE_REGISTRY.get(speaker, VOICE_REGISTRY["default"])

    try:
        _ensure_model(voice_name, voice_path)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Model unavailable: {e}")

    model_file, config_file = _model_files(voice_name, voice_path)

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
        out_path = tmp.name

    try:
        result = subprocess.run(
            [
                "piper",
                "--model",       str(model_file),
                "--config",      str(config_file),
                "--output_file", out_path,
            ],
            input=req.text.encode(),
            capture_output=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.decode())

        audio = Path(out_path).read_bytes()
        log.info("Synthesised speaker=%s len=%d chars → %d bytes WAV", speaker, len(req.text), len(audio))
        return Response(content=audio, media_type="audio/wav")

    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="TTS synthesis timed out")
    except Exception as e:
        log.exception("Synthesis failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        Path(out_path).unlink(missing_ok=True)


@app.get("/health")
def health():
    return {"status": "ok"}
