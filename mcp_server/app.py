"""
Lightweight MCP-like HTTP server exposing the existing service tools with guardrails.
Endpoints:
- GET /tools           -> list available tools
- POST /call           -> execute a tool by name with args

This is intentionally minimal and not a full MCP stdio server, but demonstrates
the same discovery/execute pattern with guardrails.
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, List

from function_calling import tools as svc_tools
from function_calling.guardrails import GuardrailError, enforce_no_injection, enforce_token_budget

app = FastAPI(title="MCP-lite Server", version="0.1.0")


class ToolSpec(BaseModel):
    name: str
    description: str
    parameters: Dict[str, Any] = {}


TOOLS: Dict[str, ToolSpec] = {
    "create_measurement": ToolSpec(
        name="create_measurement",
        description="Create measurement and start outbox + SAGA flow",
        parameters={
            "child_id": "int (required)",
            "height": "float (required)",
            "force_saga_error": "bool (optional, default False)",
        },
    ),
    "get_profiles": ToolSpec(
        name="get_profiles",
        description="Fetch child profiles",
        parameters={},
    ),
    "get_measurement_db": ToolSpec(
        name="get_measurement_db",
        description="Inspect measurement and outbox tables",
        parameters={},
    ),
}


class CallRequest(BaseModel):
    name: str
    args: Dict[str, Any] = {}
    prompt: str = ""


def _dispatch(name: str, args: Dict[str, Any]):
    if name == "create_measurement":
        return svc_tools.create_measurement(**args)
    if name == "get_profiles":
        return svc_tools.get_profiles()
    if name == "get_measurement_db":
        return svc_tools.get_measurement_db()
    raise KeyError(name)


@app.get("/tools", response_model=List[ToolSpec])
def list_tools():
    return list(TOOLS.values())


@app.post("/call")
def call_tool(req: CallRequest):
    try:
        enforce_token_budget(req.prompt or "", max_tokens=512)
        enforce_no_injection(req.prompt or "")
    except GuardrailError as e:
        raise HTTPException(status_code=400, detail=f"GuardrailError: {e}") from e

    if req.name not in TOOLS:
        raise HTTPException(status_code=404, detail="Tool not found")
    try:
        result = _dispatch(req.name, req.args or {})
    except GuardrailError as e:
        raise HTTPException(status_code=400, detail=f"GuardrailError: {e}") from e
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)) from e
    return {"tool": req.name, "result": result}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8765, log_level="info")

