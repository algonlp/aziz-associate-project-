import os
import requests

API_KEY = os.getenv("ELEVENLABS_API_KEY", "").strip()

def fetch_all_voices():
    if not API_KEY:
        raise RuntimeError("ELEVENLABS_API_KEY is not set")

    # ElevenLabs API endpoint for fetching voices
    url = "https://api.elevenlabs.io/v1/voices"

    # API headers
    headers = {
        "Content-Type": "application/json",
        "xi-api-key": API_KEY,
    }

    # Make the GET request
    response = requests.get(url, headers=headers)

    # Check the response status
    if response.status_code == 200:
        voices = response.json().get("voices", [])
        print("Fetched Voices with Details:")
        for voice in voices:
            name = voice.get("name", "Unknown")
            voice_id = voice.get("voice_id", "Unknown")
            labels = voice.get("labels", {})
            gender = labels.get("gender", "Unknown")
            accent = labels.get("accent", "Unknown")
            description = labels.get("description", "N/A")
            age = labels.get("age", "Unknown")
            use_case = labels.get("use_case", "Unknown")
            preview_url = voice.get("preview_url", "Unknown")
            

            print(f"Name: {name}")
            print(f"ID: {voice_id}")
            print(f"Gender: {gender}")
            print(f"Accent: {accent}")
            print(f"Age: {age}")
            print(f"preview_url : {preview_url}")
            print("-" * 40)
        return voices
    else:
        print(f"Error fetching voices: {response.status_code} - {response.text}")
        return []

# Run the function
if __name__ == "__main__":
    fetch_all_voices()
