import socket

import pytest

from paqto.core.errors import TransportError
from paqto.lan import endpoint_from_host_port
from paqto.lan.address import choose_advertised_host, parse_tcp_address


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


def test_public_endpoint_helper_formats_ipv6_literal() -> None:
    endpoint = endpoint_from_host_port("::1", 5050)

    assert endpoint.address == "tcp://[::1]:5050"
    assert parse_tcp_address(endpoint.address).host == "::1"


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


def test_wildcard_advertisement_does_not_open_an_internet_probe_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_socket(*args: object, **kwargs: object) -> None:
        raise AssertionError("address selection must not create a probe socket")

    def local_addresses(*args: object, **kwargs: object) -> list[tuple[object, ...]]:
        assert kwargs["family"] == socket.AF_INET
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.168.50.8", 0))]

    monkeypatch.setattr(socket, "socket", fail_socket)
    monkeypatch.setattr(socket, "getaddrinfo", local_addresses)

    assert choose_advertised_host("0.0.0.0") == (
        "192.168.50.8",
        "hostname_ipv4",
    )


def test_explicit_advertised_host_bypasses_automatic_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_resolution(*args: object, **kwargs: object) -> None:
        raise AssertionError("explicit advertisement must bypass resolution")

    monkeypatch.setattr(socket, "getaddrinfo", fail_resolution)

    assert choose_advertised_host(
        "0.0.0.0",
        advertised_host="paqto-node.local",
    ) == ("paqto-node.local", "configured")


def test_ipv6_wildcard_uses_ipv6_hostname_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def local_addresses(*args: object, **kwargs: object) -> list[tuple[object, ...]]:
        assert kwargs["family"] == socket.AF_INET6
        return [
            (
                socket.AF_INET6,
                socket.SOCK_STREAM,
                6,
                "",
                ("fd00::8", 0, 0, 0),
            )
        ]

    monkeypatch.setattr(socket, "getaddrinfo", local_addresses)

    assert choose_advertised_host("::") == ("fd00::8", "hostname_ipv6")
