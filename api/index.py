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
        import urllib.parse
        resolved_path = None
        
        # 1. Check __path query parameter from Vercel rewrite
        raw_qs = scope.get("query_string", b"").decode("latin1")
        if raw_qs:
            qs_dict = urllib.parse.parse_qs(raw_qs, keep_blank_values=True)
            if "__path" in qs_dict:
                val = qs_dict.pop("__path")[0]
                resolved_path = "/" + val.lstrip("/")
                # Reconstruct cleaned query string without __path
                new_qs_pairs = []
                for k, v_list in qs_dict.items():
                    for v in v_list:
                        new_qs_pairs.append(f"{urllib.parse.quote(k)}={urllib.parse.quote(v)}")
                scope["query_string"] = "&".join(new_qs_pairs).encode("latin1")
        
        # 2. Check x-now-route-matches or other headers if __path not found
        if not resolved_path:
            headers = dict(scope.get("headers", []))
            route_matches = headers.get(b"x-now-route-matches")
            if route_matches:
                try:
                    parsed = urllib.parse.parse_qs(route_matches.decode("latin1"))
                    if "1" in parsed and parsed["1"]:
                        val = parsed["1"][0]
                        resolved_path = "/" + val.lstrip("/")
                except Exception:
                    pass

        # 3. Fallback to scope path
        if not resolved_path:
            scope_path = scope.get("path", "/")
            if scope_path.startswith("/api/index"):
                stripped = scope_path[len("/api/index"):]
                resolved_path = "/" + stripped.lstrip("/")
            else:
        scope["path"] = resolved_path
        scope["raw_path"] = resolved_path.encode("latin1")
        scope["root_path"] = ""

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
