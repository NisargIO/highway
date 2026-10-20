#!/usr/bin/env python3
"""Local S3 contract fixture, NOT an S3 implementation or SigV4 verifier.

Atomic conditional PUT/GET, ETags, fault controls; bound to loopback. Objects
survive Highway process restarts in this test server's memory. No credentials or
message bodies are logged. Real AWS conformance is a separate opt-in command.
"""
import argparse
import json
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlsplit

objects = {}
lock = threading.Condition()
version = 0
paused = False
lose_response = False
writes = []

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *_):
        pass
    def reply(self, code, body=b'', headers=None):
        self.send_response(code)
        self.send_header('Content-Length', str(len(body)))
        for k, v in (headers or {}).items(): self.send_header(k, v)
        self.end_headers()
        try: self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError): pass
    def do_GET(self):
        path = urlsplit(self.path).path
        with lock:
            if path == '/__state':
                heads = {k: json.loads(v[0]) for k, v in objects.items() if k.endswith('/head.json')}
                indexes = {k: json.loads(v[0]) for k, v in objects.items() if '/indexes/' in k}
                self.reply(200, json.dumps({'heads': heads, 'indexes': indexes, 'keys': list(objects), 'writes': writes}).encode()); return
            if path not in objects: self.reply(404); return
            body, etag = objects[path]
        self.reply(200, body, {'ETag': etag, 'Content-Type': 'application/json'})
    def do_POST(self):
        global paused, lose_response
        if self.path != '/__control': self.reply(404); return
        data = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))))
        with lock:
            if 'pause_head' in data: paused = bool(data['pause_head'])
            if 'lose_next_head_response' in data: lose_response = bool(data['lose_next_head_response'])
            lock.notify_all()
        self.reply(200, b'{}')
    def do_PUT(self):
        global version, lose_response
        path = urlsplit(self.path).path
        length = int(self.headers.get('Content-Length', 0))
        if length > 2097152: self.reply(413); return
        body = self.rfile.read(length)
        # Verify transport preserves required signing and condition metadata.
        auth = self.headers.get('Authorization', '')
        conditional = self.headers.get('If-Match') or self.headers.get('If-None-Match')
        if not auth.startswith('AWS4-HMAC-SHA256 ') or not self.headers.get('x-amz-content-sha256') or not self.headers.get('x-amz-date') or not conditional:
            self.reply(400); return
        if self.headers.get('x-amz-security-token') != 'fixture-session': self.reply(403); return
        archival = False
        if path.endswith('/head.json'):
            h = json.loads(body)
            parts = h['archived'].split(':')
            archival = int(parts[2]) == int(h['epoch']) and int(parts[3]) > 0
        with lock:
            # Recovery heads can fence a paused old-epoch writer.
            while paused and archival: lock.wait(timeout=1)
            old = objects.get(path)
            if self.headers.get('If-None-Match') == '*' and old is not None: self.reply(412); return
            if self.headers.get('If-Match') and (old is None or old[1] != self.headers['If-Match']): self.reply(412); return
            version += 1
            etag = f'"opaque-version-{version}"'
            objects[path] = (body, etag)
            writes.append({'key': path, 'condition': conditional, 'session_token_present': True})
            lost = archival and lose_response
            if lost: lose_response = False
        if lost:
            self.close_connection = True
            self.connection.shutdown(2)
            self.connection.close()
            return
        self.reply(200, headers={'ETag': etag})

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--port', type=int, default=4402)
    args = p.parse_args()
    ThreadingHTTPServer(('127.0.0.1', args.port), Handler).serve_forever()
