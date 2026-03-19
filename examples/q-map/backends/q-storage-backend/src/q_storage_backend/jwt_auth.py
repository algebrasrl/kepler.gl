"""Re-export from shared package — do not add logic here."""
from q_backends_shared.jwt_auth import (  # noqa: F401
    JwtValidationError,
    decode_and_validate_jwt,
    extract_roles,
)
