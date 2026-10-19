import json

import pytest
from eoap_problems_registry.fastapi import (
    ForbiddenException,
    ServerErrorException,
    UnauthorizedException,
)
from fastapi import status

from app.middleware.error_handling import raise_authentication_error, raise_server_error


@pytest.mark.parametrize(
    "status_code,exception_class",
    [
        (status.HTTP_401_UNAUTHORIZED, UnauthorizedException),
        (status.HTTP_403_FORBIDDEN, ForbiddenException),
    ],
)
def test_authentication_error_preserves_context_and_cause(
    registry_request, status_code, exception_class
):
    cause = RuntimeError("Backend rejected credentials")
    with pytest.raises(exception_class) as exc_info:
        raise_authentication_error(
            registry_request, "Could not retrieve results", cause,
            status_code=status_code, headers={"WWW-Authenticate": "Bearer"},
        )

    exception = exc_info.value
    assert exception.__cause__ is cause
    assert exception.status_code == status_code
    assert exception.headers["WWW-Authenticate"] == "Bearer"
    assert json.loads(exception.detail)["errors"] == [
        {"detail": "Could not retrieve results"},
        {"detail": "Backend rejected credentials"},
    ]
    assert json.loads(exception.detail)["instance"] == "/test-request"


def test_server_error_limits_long_messages_without_losing_original_cause(registry_request):
    cause = RuntimeError("e" * 5000)
    with pytest.raises(ServerErrorException) as exc_info:
        raise_server_error(registry_request, "m" * 5000, cause)

    assert exc_info.value.__cause__ is cause
    assert json.loads(exc_info.value.detail)["errors"] == [
        {"detail": "m" * 4096}, {"detail": "e" * 4096},
    ]
