import pytest

from paqto.core.errors import TransportError
from paqto.lan.address import parse_tcp_address


@pytest.mark.parametrize(
    ("address", "host", "port"),
    [
        ("tcp://127.0.0.1:5050", "127.0.0.1", 5050),
        ("tcp://192.168.1.20:12345", "192.168.1.20", 12345),
    ],
)
def test_parse_valid_tcp_endpoint_address(
    address: str,
    host: str,
    port: int,
) -> None:
    parsed = parse_tcp_address(address)

    assert parsed.host == host
    assert parsed.port == port


@pytest.mark.parametrize(
    "address",
    [
        "udp://127.0.0.1:5050",
        "http://127.0.0.1:5050",
        "127.0.0.1:5050",
        "tcp://127.0.0.1:5050/path",
    ],
)
def test_parse_rejects_invalid_schemes_and_shapes(address: str) -> None:
    with pytest.raises(TransportError):
        parse_tcp_address(address)


@pytest.mark.parametrize(
    "address",
    [
        "tcp://127.0.0.1:-1",
        "tcp://127.0.0.1:65536",
        "tcp://127.0.0.1:not-a-port",
        "tcp://127.0.0.1:",
    ],
)
def test_parse_rejects_invalid_ports(address: str) -> None:
    with pytest.raises(TransportError):
        parse_tcp_address(address)


def test_parse_rejects_empty_address() -> None:
    with pytest.raises(TransportError):
        parse_tcp_address("")
