"""
Simple multi-tool chaining helpers to demonstrate guarded, sequential calls.
"""
from typing import List, Tuple, Callable, Any

from .guardrails import guarded_call


def run_chain(prompt: str, steps: List[Tuple[Callable, dict]], max_tokens: int = 512) -> Any:
    """
    Run a sequence of tool calls under guardrails.
    steps: list of (tool_fn, kwargs)
    """
    result = None
    for tool_fn, kwargs in steps:
        result = guarded_call(prompt, tool_fn, max_tokens=max_tokens, **kwargs)
    return result



