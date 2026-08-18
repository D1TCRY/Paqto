"""Import compatibility for the current async connection abstraction.

The canonical definition lives in :mod:`paqto.core.connection`. This module is
kept so existing ``from paqto.connection import Connection`` imports resolve to
the same public type as ``from paqto import Connection``. It does not emulate
the retired threaded ``Connection(host, port)`` constructor or blocking API.
"""

from paqto.core.connection import Connection, ConnectionState
from paqto.core.security import SecurityInfo

__all__ = ["Connection", "ConnectionState", "SecurityInfo"]
