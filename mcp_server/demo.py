"""
Demo: issue requests to a running MCP-lite server.

Prereqs:
- Services running: `docker-compose up --build`
- MCP-lite server running separately, e.g.:
    uvicorn mcp_server.app:app --host 127.0.0.1 --port 8765 --log-level warning

Run:
    python -m mcp_server.demo
"""
import sys
import requests


def demo_calls(height: float):
    base = "http://127.0.0.1:8765"

    print("\n[List tools]")
    r = requests.get(f"{base}/tools", timeout=5)
    print(r.json())

    print("\n[Call create_measurement]")
    r = requests.post(
        f"{base}/call",
        json={
            "name": "create_measurement",
            "args": {"child_id": 1, "height": height, "force_saga_error": False},
            "prompt": f"Add measurement {height}cm and proceed.",
        },
        timeout=10,
    )
    print(r.json())

    print("\n[Call get_profiles]")
    r = requests.post(
        f"{base}/call",
        json={
            "name": "get_profiles",
            "args": {},
            "prompt": "Fetch profiles.",
        },
        timeout=10,
    )
    print(r.json())


if __name__ == "__main__":
    height = 136.0
    if len(sys.argv) > 1:
        try:
            height = float(sys.argv[1])
        except ValueError:
            print("Invalid height argument, using default 136.0")
    demo_calls(height)

