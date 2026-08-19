# Built-in serializers

Paqto includes safe, dependency-free serializers for common payloads while
keeping serializer selection explicit. Import them from `paqto.serializers`
and pass one to `PaqtoNode`:

```python
from paqto import PaqtoNode
from paqto.serializers import JsonSerializer

node = PaqtoNode(
    name="device-a",
    transport=transport,
    discovery=discovery,
    serializer=JsonSerializer(),
)
```

There is no implicit serializer. The selected serializer defines the
application wire format, and both peers must advertise the same
`protocol_id` during the Paqto handshake.

## `JsonSerializer`

`JsonSerializer` uses deterministic UTF-8 JSON and has the stable protocol id
`paqto.message-json.v1`. It supports payloads composed only of:

- `None`;
- booleans;
- finite integers and floats;
- strings;
- lists;
- dictionaries with string keys.

It deliberately rejects bytes, tuples, non-finite floats, non-string mapping
keys, and arbitrary Python objects rather than silently changing their types.

The serializer validates configurable payload limits before serialization and
after deserialization:

```python
serializer = JsonSerializer(
    max_nesting=32,
    max_collection_items=10_000,
    max_string_length=100_000,
)
```

The defaults are `64`, `100_000`, and `1_000_000`, respectively. Collection
items are counted across the complete payload. These object limits complement,
but do not replace, `PaqtoConfig.max_message_size`, which limits serialized
application bytes before deserialization.

## `BytesSerializer`

`BytesSerializer` accepts only an exact `bytes` payload. It uses a deterministic
JSON envelope with canonical Base64 payload data and has the stable protocol id
`paqto.message-bytes.v1`:

```python
from paqto.serializers import BytesSerializer

node = PaqtoNode(
    name="binary-device",
    transport=transport,
    discovery=discovery,
    serializer=BytesSerializer(),
)

await node.send(peer, b"\x00\x01\x02", type="binary.chunk")
```

Base64 makes the format portable and dependency-free, but increases payload
size. The negotiated message limit applies to the complete encoded envelope,
not only to the original bytes.

## Envelope and safety guarantees

Both built-in serializers:

- preserve every public `Message` field, including `created_at`, `id`, and
  `reply_to`;
- require timezone-aware `created_at` values;
- reject missing or unexpected envelope fields;
- reject duplicate JSON object keys, malformed UTF-8, malformed JSON, and
  non-finite JSON constants;
- raise `SerializationError` for expected conversion failures;
- never use `pickle` or execute data received from the network.

Payload validation is not application authorization or schema validation.
Applications should still validate allowed message types, field meanings, and
operation-specific limits before performing side effects.

## Custom serializers

Applications can continue to implement the public `Serializer` contract:

```python
from paqto import Message, Serializer


class ApplicationSerializer(Serializer):
    @property
    def protocol_id(self) -> str:
        return "example/application-format.v1"

    def serialize(self, message: Message) -> bytes:
        ...

    def deserialize(self, data: bytes) -> Message:
        ...
```

A custom serializer must preserve the complete envelope and return `bytes`
from `serialize()` and `Message` from `deserialize()`. Use a stable, versioned
protocol id whenever independent processes or languages must interoperate.
Never deserialize untrusted network data with unsafe object-construction
formats such as `pickle`.
