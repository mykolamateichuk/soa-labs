import re
from typing import Callable


class GuardrailError(Exception):
    """Raised when guardrails are violated."""


def enforce_token_budget(text: str, max_tokens: int = 512) -> None:
    """
    Very lightweight token-budget guard: counts word-like tokens.
    Raises GuardrailError if over budget.
    """
    token_count = len(re.findall(r"\w+", text))
    if token_count > max_tokens:
        raise GuardrailError(f"Token budget exceeded: {token_count}>{max_tokens}")


def detect_prompt_injection(text: str) -> bool:
    """
    Flag obvious prompt-injection attempts with simple heuristics.
    """
    lowered = text.lower()
    bad_markers = [
        "ignore previous instructions",
        "disregard earlier",
        "system prompt",
        "override",
        "jailbreak",
        "pretend to be",
        "ignore all",
    ]
    return any(marker in lowered for marker in bad_markers)


def enforce_no_injection(text: str) -> None:
    """
    Raise GuardrailError on suspected prompt injection.
    """
    if detect_prompt_injection(text):
        raise GuardrailError("Prompt rejected by injection guard.")


def guarded_call(
    prompt: str,
    tool_fn: Callable,
    *,
    max_tokens: int = 512,
    **tool_kwargs,
):
    """
    Apply guardrails, then invoke the tool.
    """
    enforce_token_budget(prompt, max_tokens=max_tokens)
    enforce_no_injection(prompt)
    return tool_fn(**tool_kwargs)



