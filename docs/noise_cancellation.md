# Noise Cancellation Strategy

## Goals
- Keep Twilio inbound audio as clean as possible before it reaches ElevenLabs’ realtime recogniser.
- Avoid false barge-ins when callers click a mouse, tap a keyboard, or shuffle a headset.
- Stay realtime: the filter must not add more than the 10–20 ms of buffering already present in the stream.

## Multi-stage approach
1. **Stream-aligned framing.** Twilio mu-law audio is decoded and upsampled to 16 kHz in `TranscriptionService`. We analyse the same stream in 20 ms windows with a 10 ms hop so there’s never more than a single-frame delay.
2. **RNNoise-inspired spectral score.** Jean-Marc Valin’s [RNNoise paper](https://arxiv.org/abs/1709.08243) shows that crest factor + band-limited spectral energy are reliable speech cues. We compute those statistics with NumPy (no external binary) and only release frames whose confidence score passes the `SPEECH_CONFIDENCE_MIN` threshold.
3. **WebRTC-VAD gating.** WebRTC’s VAD still runs on every frame, just like the Vocode telephony examples that rely on `PunctuationEndpointingConfig`. The spectral score and the VAD result are blended, so short impulses are ignored while genuine speech flows immediately.
4. **Adaptive noise profile.** Every non-speech frame updates a rolling RMS baseline. When speech starts we compare the instantaneous SNR to that baseline so humming air conditioners never trigger a pause.
5. **Legacy overlap-add suppressor.** The existing `NoiseSuppressor` still runs to tame steady background noise. If the spectral score ever fails we fall back to this suppressor automatically.

## Runtime controls
All knobs are optional environment variables with safe defaults:

- `ENABLE_NOISE_SUPPRESSION` – master switch (default `true`).
- `NOISE_SUPPRESSOR_FRAME_MS` / `NOISE_SUPPRESSOR_HOP_MS` – keep these at 20/10 ms for perfect overlap-add reconstruction.
- `NOISE_SUPPRESSOR_STRENGTH`, `NOISE_SUPPRESSOR_SPEECH_GAIN_FLOOR`, `NOISE_SUPPRESSOR_SILENCE_GAIN_FLOOR`, `NOISE_PROFILE_SMOOTHING`, `NOISE_CLICK_GUARD_LEVEL`, `TRANSIENT_SUPPRESS_MS` – classic overlap-add controls.
- **Spectral confidence knobs**
  - `SPEECH_CONFIDENCE_MIN` – blended score threshold (default `0.5`).
  - `SPEECH_SNR_MIN_DB` / `SPEECH_SNR_TARGET_DB` – when SNR is above these limits the confidence score grows faster (defaults `3`/`12` dB).
  - `SPEECH_VOICE_RATIO_MIN` / `SPEECH_VOICE_RATIO_MAX` – acceptable fraction of energy inside the 200–3600 Hz “voice band” (defaults `0.15`/`0.9`).
  - `SPEECH_CREST_MAX` – maximum crest factor before we treat a frame as an impulse (default `18`).
  - `SPEECH_NOISE_BASELINE` / `SPEECH_NOISE_DECAY` – governs how quickly the adaptive noise floor tracks new environments.
  - `SPEECH_VOICE_BAND_LOW_HZ` / `SPEECH_VOICE_BAND_HIGH_HZ` – bounds for the speech-dominant frequency range.

## Deployment notes
- Everything lives in `services/transcription_service.py` and `services/noise_suppression.py`. The gate is stateful per Twilio stream; `set_stream_sid` and `disconnect` reset it so concurrent calls stay isolated.
- There are no third-party binaries or paid SDKs in this pipeline—just NumPy and WebRTC VAD.
- If you need to disable the filter for diagnostics, set `ENABLE_NOISE_SUPPRESSION=false` to fall back to raw Twilio audio.
