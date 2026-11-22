class ClientNotFound(Exception):
    """Raised when no client has that key (or that key hash) in the local DB."""


class ClientInactive(Exception):
    """Raised when the client exists but is_active=False."""
