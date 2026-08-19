"""Strict JSON helpers shared by built-in serializers."""

from __future__ import annotations

import json
import math
from typing import Any

from paqto.core.errors import SerializationError


def encode_json(value: object) -> bytes:
    """Encode deterministic, finite JSON as UTF-8 bytes."""
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (RecursionError, TypeError, ValueError) as exc:
        raise SerializationError("Message is not valid JSON data.") from exc


def decode_json(data: bytes) -> object:
    """Decode UTF-8 JSON while rejecting duplicate keys and non-finite numbers."""
    if not isinstance(data, bytes):
        raise SerializationError("Serialized message data must be bytes.")
    try:
        return json.loads(
            data.decode("utf-8"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (RecursionError, UnicodeDecodeError, ValueError) as exc:
        raise SerializationError("Serialized message is not valid UTF-8 JSON.") from exc


def validate_json_value(
    value: object,
    *,
    max_nesting: int,
    max_collection_items: int,
    max_string_length: int,
) -> None:
    """Validate a portable JSON value and configured resource limits."""
    pending: list[tuple[object, int]] = [(value, 0)]
    collection_items = 0
    while pending:
        item, depth = pending.pop()
        if item is None or isinstance(item, bool):
            continue
        if isinstance(item, int):
            continue
        if isinstance(item, float):
            if not math.isfinite(item):
                raise SerializationError("JSON payload numbers must be finite.")
            continue
        if isinstance(item, str):
            if len(item) > max_string_length:
                raise SerializationError(
                    f"JSON payload string exceeds {max_string_length} characters."
                )
            continue
        if isinstance(item, list):
            if depth >= max_nesting:
                raise SerializationError(
                    f"JSON payload exceeds maximum nesting of {max_nesting}."
                )
            collection_items += len(item)
            pending.extend((nested, depth + 1) for nested in item)
        elif isinstance(item, dict):
            if depth >= max_nesting:
                raise SerializationError(
                    f"JSON payload exceeds maximum nesting of {max_nesting}."
                )
            if any(not isinstance(key, str) for key in item):
                raise SerializationError("JSON payload object keys must be strings.")
            for key in item:
                if len(key) > max_string_length:
                    raise SerializationError(
                        "JSON payload object key exceeds "
                        f"{max_string_length} characters."
                    )
            collection_items += len(item)
            pending.extend((nested, depth + 1) for nested in item.values())
        else:
            raise SerializationError(
                f"JSON payload does not support {type(item).__name__} values."
            )
        if collection_items > max_collection_items:
            raise SerializationError(
                "JSON payload exceeds maximum total collection items of "
                f"{max_collection_items}."
            )


def validate_limit(name: str, value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer.")
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero.")


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON object key {key!r}.")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"Non-finite JSON number {value!r} is not supported.")
