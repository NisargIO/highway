#!/usr/bin/env python3
"""Apply the reviewed, version-locked Mist input cap before building.

Mist 6.0.3 buffers incomplete WebSocket frames without a size limit. Keep the
minimal upstream patch in patches/ and fail on an unexpected dependency source.
"""
from hashlib import sha256
from pathlib import Path
import subprocess

path = Path('build/packages/mist/src/mist/internal/websocket.gleam')
original = 'c419b32f63df86768eac89874f700c3907a85bd656e57c68e984f9aedf9ba7e3'
patched = '25a9d0041887e0fb20ea0bc15befea3edc742c7f4a380513607d86acdca96096'
if not path.exists():
    raise SystemExit('Run make setup first')
digest = sha256(path.read_bytes()).hexdigest()
if digest == original:
    subprocess.run(['patch', '--forward', '-p1', '-i', 'patches/mist-input-cap.patch'], check=True)
elif digest != patched:
    raise SystemExit('Unexpected Mist source: review the locked dependency and input cap patch')
if sha256(path.read_bytes()).hexdigest() != patched:
    raise SystemExit('Mist input cap verification failed')
