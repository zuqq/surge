import asyncio
import http.server
import struct

from surge import bencoding


class HTTPTracker(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(
            bencoding.encode(
                {
                    b"interval": 900,
                    b"peers": struct.pack(">BBBBH", 127, 0, 0, 1, 6881),
                }
            )
        )

    def log_message(self, format, *args):
        pass


async def serve_peers_http():
    with http.server.HTTPServer(("127.0.0.1", 8080), HTTPTracker) as server:
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, server.serve_forever)
        finally:
            await loop.run_in_executor(None, server.shutdown)
