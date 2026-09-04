import os
import sys
import traceback
from pathlib import Path

# Add project root to sys.path
root_dir = Path(__file__).resolve().parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

os.environ["VERCEL"] = "1"

try:
    from src.api.main import app as fastapi_app
    _import_error = None
except Exception:
    fastapi_app = None
    _import_error = traceback.format_exc()


async def app(scope, receive, send):
    if _import_error:
        response_body = f"<h1>Import Error on Vercel</h1><pre>{_import_error}</pre>".encode("utf-8")
        await send({
            "type": "http.response.start",
            "status": 500,
            "headers": [
                (b"content-type", b"text/html; charset=utf-8"),
                (b"content-length", str(len(response_body)).encode("ascii")),
            ],
        })
        await send({
            "type": "http.response.body",
            "body": response_body,
        })
        return

    if scope["type"] == "http":
        path = scope.get("path", "/")
        # Strip /api/index prefix if added by Vercel rewrite
        if path.startswith("/api/index"):
            new_path = path[len("/api/index"):]
            if not new_path or not new_path.startswith("/"):
                new_path = "/" + new_path
            scope["path"] = new_path

    try:
        await fastapi_app(scope, receive, send)
    except Exception:
        err = traceback.format_exc()
        response_body = f"<h1>Runtime Execution Error on Vercel</h1><pre>{err}</pre>".encode("utf-8")
        await send({
            "type": "http.response.start",
            "status": 500,
            "headers": [
                (b"content-type", b"text/html; charset=utf-8"),
                (b"content-length", str(len(response_body)).encode("ascii")),
            ],
        })
        await send({
            "type": "http.response.body",
            "body": response_body,
        })
