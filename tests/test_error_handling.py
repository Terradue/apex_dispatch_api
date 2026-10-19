from fastapi import status
import json
from pathlib import Path

import pytest
from eoap_problems_registry import NotFound
from eoap_problems_registry.fastapi import ProblemRegistryException
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.testclient import TestClient
from pydantic import BaseModel, field_validator

from app.middleware.correlation_id import add_correlation_id
from app.middleware.error_handling import PROBLEM_RESPONSES, register_exception_handlers


@pytest.fixture
def problem_client():
    app = FastAPI(responses=PROBLEM_RESPONSES)
    app.middleware("http")(add_correlation_id)
    register_exception_handlers(app)

    class Payload(BaseModel):
        value: int

        @field_validator("value")
        @classmethod
        def positive(cls, value):
            if value < 0:
                raise ValueError("must be positive")
            return value

    @app.post("/validate")
    def validate(payload: Payload):
        return payload

    @app.get("/registered")
    def registered(request: Request):
        raise ProblemRegistryException(
            request=request, problem=NotFound(), headers={"X-Test": "kept"}
        )

    @app.get("/http/{code}")
    def http(code: int):
        raise HTTPException(code, "Request failed", headers={"WWW-Authenticate": "Bearer"})

    @app.get("/unexpected")
    def unexpected():
        raise RuntimeError("Processing service failed")

    def consume_header(x_client: str = Header(alias="X-Client")):
        return x_client

    @app.get("/resources/{resource_id}/parts/{part_id}")
    def resource(
        request: Request, resource_id: int, part_id: str,
        client: str = Depends(consume_header),
        x_mode: str = Header(alias="X-Mode", default="default"),
    ):
        raise ProblemRegistryException(request=request, problem=NotFound())

    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


def test_registry_response_context_and_headers(problem_client):
    response = problem_client.get("/registered?token=secret", headers={"X-Correlation-ID": "abc"})
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.headers["content-type"] == "application/problem+json"
    assert response.headers["X-Test"] == "kept"
    assert response.headers["X-Correlation-ID"] == "abc"
    assert response.json() == {
        **NotFound().model_dump(mode="json", exclude_none=True),
        "instance": "http://testserver/registered", "request_id": "abc",
        "errors": [{"header": "X-Correlation-ID", "detail": "abc"}],
    }


@pytest.mark.parametrize(
    "code",
    [
        status.HTTP_400_BAD_REQUEST,
        status.HTTP_401_UNAUTHORIZED,
        status.HTTP_403_FORBIDDEN,
        status.HTTP_404_NOT_FOUND,
        status.HTTP_405_METHOD_NOT_ALLOWED,
        status.HTTP_422_UNPROCESSABLE_ENTITY,
        status.HTTP_500_INTERNAL_SERVER_ERROR,
        status.HTTP_503_SERVICE_UNAVAILABLE,
    ],
)
def test_http_status_and_headers_preserved(problem_client, code):
    response = problem_client.get(f"/http/{code}")
    assert response.status_code == code
    assert response.json()["status"] == code
    assert response.headers["content-type"] == "application/problem+json"
    assert response.headers["WWW-Authenticate"] == "Bearer"


def test_validation_does_not_serialize_exception_context_or_input(problem_client):
    response = problem_client.post("/validate", json={"value": -1, "secret": "private"})
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    problem = response.json()
    assert problem["type"].endswith("/validation-error")
    assert problem["errors"] == [{"detail": "Value error, must be positive", "pointer": "/value"}]
    assert "private" not in response.text


def test_unexpected_error_includes_exception_detail(problem_client):
    response = problem_client.get("/unexpected", headers={"X-Correlation-ID": "unexpected-id"})
    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert response.json()["type"].endswith("/server-error")
    assert response.json()["request_id"] == "unexpected-id"
    assert response.json()["errors"] == [
        {"detail": "Processing service failed"},
        {"header": "X-Correlation-ID", "detail": "unexpected-id"},
    ]


def test_openapi_reuses_registry_responses():
    document = json.loads(Path("openapi.json").read_text())
    assert "ErrorResponse" not in document["components"]["schemas"]
    for item in document["paths"].values():
        for method, operation in item.items():
            if method not in {"get", "post", "delete", "put", "patch"}:
                continue
            for code, response in operation["responses"].items():
                if code in {str(status_code) for status_code in PROBLEM_RESPONSES}:
                    name = response["$ref"].rsplit("/", 1)[1]
                    assert document["components"]["responses"][name]["$ref"].startswith(
                        "https://eoap.github.io/problems-registry/"
                    )


@pytest.mark.asyncio
@pytest.mark.parametrize("scope_type,scheme", [("http", "https"), ("websocket", "wss")])
async def test_service_request_context_preserves_url_and_is_reset(scope_type, scheme):
    from eoap_problems_registry.fastapi import UnauthorizedException
    from app.middleware.request_context import RequestContextMiddleware, request_context

    scope = {
        "type": scope_type, "scheme": scheme, "path": "/protected",
        "headers": [(b"host", b"example.test")], "query_string": b"token=secret",
    }
    previous = request_context.get(None)

    async def endpoint(scope, receive, send):
        raise UnauthorizedException(request=request_context.get())

    async def receive():
        return {}

    async def send(message):
        pass

    with pytest.raises(UnauthorizedException) as exc_info:
        await RequestContextMiddleware(endpoint)(scope, receive, send)

    assert json.loads(exc_info.value.detail)["instance"] == f"{scheme}://example.test/protected"
    assert request_context.get(None) is previous


@pytest.mark.asyncio
async def test_http_adapter_delegates_to_registry_handler():
    from unittest.mock import AsyncMock, patch
    from app.middleware.error_handling import http_exception_handler

    request = Request({"type": "http", "path": "/protected", "headers": []})
    exception = HTTPException(
        status.HTTP_401_UNAUTHORIZED, "Missing credentials", headers={"WWW-Authenticate": "Bearer"}
    )
    with patch(
        "app.middleware.error_handling.problem_registry_exception_handler",
        new_callable=AsyncMock,
    ) as handler:
        response = await http_exception_handler(request, exception)

    assert response is handler.return_value
    handler.assert_awaited_once()
    forwarded_request, forwarded_exception = handler.call_args.args
    assert forwarded_request is request
    assert isinstance(forwarded_exception, ProblemRegistryException)
    assert json.loads(forwarded_exception.detail)["type"].endswith("/unauthorized")
    assert forwarded_exception.headers["WWW-Authenticate"] == "Bearer"
    assert json.loads(forwarded_exception.detail)["errors"] == [{"detail": "Missing credentials"}]


@pytest.mark.asyncio
async def test_http_adapter_preserves_existing_registry_problem():
    from app.middleware.error_handling import http_exception_handler

    request = Request({"type": "http", "path": "/missing", "headers": []})
    problem = NotFound(code="JOB_NOT_FOUND", request_id="original-id")
    exception = ProblemRegistryException(
        request=request, problem=problem, headers={"X-Test": "kept"}
    )
    response = await http_exception_handler(request, exception)

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.headers["X-Test"] == "kept"
    assert json.loads(response.body) == problem.model_dump(mode="json", exclude_none=True)


def test_framework_not_found_uses_registry_problem(problem_client):
    response = problem_client.get("/missing")
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["type"].endswith("/not-found")


@pytest.mark.parametrize("code", [status.HTTP_502_BAD_GATEWAY, status.HTTP_504_GATEWAY_TIMEOUT])
def test_unmapped_http_error_uses_registry_server_error(problem_client, code):
    response = problem_client.get(
        f"/http/{code}?token=secret", headers={"X-Correlation-ID": "fallback-id"}
    )
    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert response.headers["content-type"] == "application/problem+json"
    assert response.headers["WWW-Authenticate"] == "Bearer"
    problem = response.json()
    assert problem["type"].endswith("/server-error")
    assert problem["status"] == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert problem["instance"] == f"http://testserver/http/{code}"
    assert problem["request_id"] == "fallback-id"
    assert problem["errors"] == [
        {"detail": "Request failed"},
        {"parameter": "code", "detail": str(code)},
        {"header": "X-Correlation-ID", "detail": "fallback-id"},
    ]


def test_path_parameters_and_consumed_headers_are_attached(problem_client):
    response = problem_client.get(
        "/resources/42/parts/image",
        headers={"X-Client": "test-client", "X-Mode": "preview", "X-Unused": "omit-me"},
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["errors"] == [
        {"parameter": "resource_id", "detail": "42"},
        {"parameter": "part_id", "detail": "image"},
        {"header": "X-Client", "detail": "test-client"},
        {"header": "X-Mode", "detail": "preview"},
    ]


def test_validation_errors_also_include_supplied_request_values(problem_client):
    response = problem_client.get("/resources/not-an-int/parts/image", headers={"X-Client": "cli"})
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    errors = response.json()["errors"]
    assert {"parameter": "resource_id", "detail": "not-an-int"} in errors
    assert {"parameter": "part_id", "detail": "image"} in errors
    assert {"header": "X-Client", "detail": "cli"} in errors
    assert not any(error.get("header") == "X-Mode" for error in errors)


def test_request_context_details_are_not_duplicated():
    from app.middleware.error_handling import add_request_error_details

    request = Request({
        "type": "http", "path": "/jobs/42", "path_params": {"job_id": 42}, "headers": [],
    })
    exception = ProblemRegistryException(request=request, problem=NotFound())
    add_request_error_details(request, exception)
    add_request_error_details(request, exception)
    assert json.loads(exception.detail)["errors"] == [{"parameter": "job_id", "detail": "42"}]
