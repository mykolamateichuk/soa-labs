"""
Minimal tool wrappers that use existing services in this repo.
They are simple functions so an LLM function-calling layer can invoke them.
"""
import requests
from typing import Any, Dict, Optional

MEASUREMENT_BASE = "http://localhost:8003"
PROFILE_BASE = "http://localhost:8002"


def create_measurement(
    child_id: int,
    height: float,
    force_saga_error: bool = False,
) -> Dict[str, Any]:
    url = f"{MEASUREMENT_BASE}/measure"
    resp = requests.post(
        url,
        json={
            "child_id": child_id,
            "height": height,
            "force_saga_error": force_saga_error,
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def get_measurement_db() -> Dict[str, Any]:
    url = f"{MEASUREMENT_BASE}/db/data"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_profiles() -> Dict[str, Any]:
    url = f"{PROFILE_BASE}/profiles"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def update_profile(
    child_id: int,
    height: float,
    force_error: bool = False,
) -> Dict[str, Any]:
    url = f"{PROFILE_BASE}/profiles/{child_id}"
    resp = requests.put(
        url,
        params={"height": height, "force_error": force_error},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()



