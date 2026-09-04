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
        headers = dict(scope.get("headers", []))
        raw_path = scope.get("path", "/")
        
        # Immediate debug probe
        if b"/debug-headers" in raw_path.encode("utf-8") or any(b"debug-headers" in v for v in headers.values()):
            debug_info = {
                "scope_path": raw_path,
                "scope_raw_path": scope.get("raw_path", b"").decode("utf-8", errors="ignore"),
                "headers": {k.decode("latin1"): v.decode("latin1") for k, v in headers.items()}
            }
            import json
            body = json.dumps(debug_info, indent=2).encode("utf-8")
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            })
            await send({
                "type": "http.response.body",
                "body": body,
            })
            return
        headers = dict(scope.get("headers", []))
        matched = False
        for h_name in (b"x-matched-path", b"x-forwarded-uri", b"x-invoke-path", b"x-original-url", b"x-rewrite-url"):
            val = headers.get(h_name)
            if val:
                decoded = val.decode("utf-8", errors="ignore")
                if decoded and not decoded.startswith("/api/index"):
                    path_only = decoded.split("?")[0]
                    scope["path"] = path_only
                    matched = True
                    break
        
        if not matched:
            path = scope.get("path", "/")
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
