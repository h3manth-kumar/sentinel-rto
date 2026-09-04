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
        
        # 1. Check x-now-route-matches (Vercel rewrite regex capture group $1)
        resolved_path = None
        route_matches = headers.get(b"x-now-route-matches")
        if route_matches:
            try:
                import urllib.parse
                parsed = urllib.parse.parse_qs(route_matches.decode("latin1"))
                if "1" in parsed and parsed["1"]:
                    val = parsed["1"][0]
                    resolved_path = "/" + val.lstrip("/")
            except Exception:
                pass

        # 2. Check x-forwarded-url / x-forwarded-uri / x-original-url / x-invoke-path
        if not resolved_path:
            import urllib.parse
            for h in (b"x-forwarded-url", b"x-forwarded-uri", b"x-original-url", b"x-invoke-path", b"x-rewrite-url"):
                val = headers.get(h)
                if val:
                    decoded = val.decode("latin1", errors="ignore")
                    if decoded.startswith("http://") or decoded.startswith("https://"):
                        url_path = urllib.parse.urlparse(decoded).path
                        if url_path:
                            resolved_path = url_path
                            break
                    elif decoded and not decoded.startswith("/api/index"):
                        resolved_path = decoded.split("?")[0]
                        break

        # 3. Fallback to scope path
        if not resolved_path:
            scope_path = scope.get("path", "/")
            if scope_path.startswith("/api/index"):
                stripped = scope_path[len("/api/index"):]
                resolved_path = "/" + stripped.lstrip("/")
            else:
                resolved_path = scope_path

        scope["path"] = resolved_path

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
