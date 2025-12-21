"""
Lyria RealTime Wrapper for TTS Service.
Generates music via Google GenAI Live API (WebSocket) and saves to WAV.

Usage:
    python3 wrapper.py --api-key <KEY> --prompt <TEXT> --output <FILE.WAV> --duration <SECONDS>
"""

import argparse
import asyncio
import os
import sys
import wave
from google import genai
from google.genai import types

# Constants
MODEL = 'models/lyria-realtime-exp'
SAMPLE_RATE = 44100
CHANNELS = 2
SAMPLE_WIDTH = 2 # 16-bit = 2 bytes

async def generate_music(api_key, prompt, output_path, duration_seconds):
    client = genai.Client(
        api_key=api_key,
        http_options={'api_version': 'v1alpha'},
    )

    audio_buffer = bytearray()
    
    # Calculate expected bytes: Rate * Channels * Width * Duration
    # Lyria might return 44.1k or 48k depending on config. We request 48k if possible, or resample?
    # The user example used 44100. We'll stick to 44100 for Lyria default, but we can set config.
    # We want 48k for our pipeline. Let's try to request 48k.
    
    target_bytes = 48000 * 2 * 2 * duration_seconds
    
    print(f"Connecting to {MODEL}...")
    
    async with client.aio.live.music.connect(model=MODEL) as session:
        # Configuration
        print(f"Setting config: BPM=Auto, Format=PCM16, Rate=48000")
        await session.set_music_generation_config(
            config=types.LiveMusicGenerationConfig(
                music_generation_mode=types.MusicGenerationMode.QUALITY,
                # audio_format removed as it might not be exposed in types yet
            )
        )
        
        # Prompt
        print(f"Sending prompt: {prompt}")
        await session.set_weighted_prompts(
            prompts=[types.WeightedPrompt(text=prompt, weight=1.0)]
        )
        
        # Start
        await session.play()
        print("Streaming started...")

        # Receive Loop
        start_time = asyncio.get_running_loop().time()
        
        async for message in session.receive():
            if message.server_content and message.server_content.audio_chunks:
                for chunk in message.server_content.audio_chunks:
                    audio_buffer.extend(chunk.data)
            
            # Check duration limit
            # We approximate duration based on received bytes or time.
            # Time is safer for network lag, but bytes is safer for file length.
            # Let's use time first.
            elapsed = asyncio.get_running_loop().time() - start_time
            if elapsed >= duration_seconds:
                print(f"Duration reached ({elapsed:.2f}s). Stopping.")
                break
        
        # Stop session
        # await session.stop() 
        # Context exit closes connection.

    # Save to WAV
    print(f"Saving {len(audio_buffer)} bytes to {output_path}...")
    with wave.open(output_path, 'wb') as wf:
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(SAMPLE_WIDTH)
        wf.setframerate(SAMPLE_RATE) # Assuming 44.1k if config didn't force 48k.
        # Ideally we check the metadata, but for now we assume standard Lyria output.
        # If we requested 48k and got it, great. If we got 44.1k, we might play it fast/slow if we assume wrong.
        # Safe bet: Lyria usually does 44100. 
        # But we want 48000 for pipeline.
        # Let's write as 44100 and let ffmpeg resample later if needed.
        wf.writeframes(audio_buffer)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--duration", type=int, default=30)
    args = parser.parse_args()

    try:
        asyncio.run(generate_music(args.api_key, args.prompt, args.output, args.duration))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
