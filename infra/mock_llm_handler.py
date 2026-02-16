from __future__ import annotations

import json
import re
from typing import Any, Dict, List

import uuid6


async def mock_chat(messages: List[Dict[str, Any]], model: str) -> Dict[str, Any]:
    if model in ("mock-direct", "mock-answer"):
        last_user = next((m for m in reversed(messages) if m.get("role") == "user"), None) or {}
        text = str(last_user.get("content") or "")
        m = re.search(r"(-?\d+)\s*([+\-*/])\s*(-?\d+)", text)
        if m:
            a = int(m.group(1))
            op = m.group(2)
            b = int(m.group(3))
            if op == "+":
                ans = a + b
            elif op == "-":
                ans = a - b
            elif op == "*":
                ans = a * b
            else:
                ans = "0" if b == 0 else str(a / b)
            out = str(ans)
        else:
            out = "ok"

        return {
            "content": "",
            "tool_calls": [
                {
                    "function": {
                        "name": "submit_result",
                        "arguments": json.dumps(
                            {"result": {"fields": [{"name": "output", "value": out}]}}
                        ),
                    },
                    "id": f"call_mock_submit_{uuid6.uuid7().hex}",
                    "type": "function",
                }
            ],
        }

    has_error = any(
        msg.get("role") == "tool" and "failed" in str(msg.get("content"))
        for msg in messages
    )
    if has_error:
        return {
            "content": "I apologize, the tool failed. I will finish now.",
            "tool_calls": [
                {
                    "function": {
                        "name": "submit_result",
                        "arguments": json.dumps(
                            {
                                "result": {
                                    "fields": [
                                        {
                                            "name": "output",
                                            "value": "I tried to use a tool but it failed. (Recovery Test Successful)",
                                        }
                                    ]
                                },
                            }
                        ),
                    },
                    "id": f"call_mock_recovery_{uuid6.uuid7().hex}",
                    "type": "function",
                }
            ],
        }

    return {
        "content": "I am testing the safety mechanism.",
        "tool_calls": [
            {
                "function": {
                    "name": "trigger_error_tool",
                    "arguments": json.dumps({"param": "value"}),
                },
                "id": f"call_mock_fail_{uuid6.uuid7().hex}",
                "type": "function",
            }
        ],
    }
