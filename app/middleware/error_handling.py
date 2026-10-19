from fastapi import status
import json
import traceback
from typing import Any, NoReturn

import eoap_problems_registry as registry
from eoap_problems_registry.fastapi import (
    ProblemRegistryException,
    BadRequestException,
    ForbiddenException,
    MethodNotAllowedException,
    NotFoundException,
    ServerErrorException,
    ServiceUnavailableException,
    UnauthorizedException,
    UnprocessableContentException,
    ValidationErrorException,
    problem_registry_exception_handler,
)
from fastapi import Request
from fastapi.security import OAuth2
from fastapi.exceptions import RequestValidationError
from loguru import logger
from starlette.exceptions import HTTPException

from app.middleware.correlation_id import correlation_id_ctx


PROBLEM_RESPONSES: dict[int | str, dict[str, Any]] = {
    code: {"$ref": f"https://eoap.github.io/problems-registry/openapi.yaml"
           f"#/components/responses/{name}"}
    for code, name in (
        (status.HTTP_400_BAD_REQUEST, "BadRequest"),
        (status.HTTP_401_UNAUTHORIZED, "Unauthorized"),
        (status.HTTP_403_FORBIDDEN, "Forbidden"),
        (status.HTTP_404_NOT_FOUND, "NotFound"),
        (status.HTTP_422_UNPROCESSABLE_ENTITY, "ValidationError"),
        (status.HTTP_500_INTERNAL_SERVER_ERROR, "ServerError"),
    )
}


HTTP_PROBLEM_EXCEPTIONS = {
    status.HTTP_400_BAD_REQUEST: BadRequestException,
    status.HTTP_401_UNAUTHORIZED: UnauthorizedException,
    status.HTTP_403_FORBIDDEN: ForbiddenException,
    status.HTTP_404_NOT_FOUND: NotFoundException,
    status.HTTP_405_METHOD_NOT_ALLOWED: MethodNotAllowedException,
    status.HTTP_422_UNPROCESSABLE_ENTITY: UnprocessableContentException,
    status.HTTP_500_INTERNAL_SERVER_ERROR: ServerErrorException,
    status.HTTP_503_SERVICE_UNAVAILABLE: ServiceUnavailableException,
}


def _error_details(message: str, cause: Exception | None = None) -> list[registry.ErrorDetail]:
    errors = [registry.ErrorDetail(detail=message[:4096])]
    if cause is not None:
        errors.append(registry.ErrorDetail(detail=str(cause)[:4096]))
    return errors


def raise_server_error(request: Request, message: str, cause: Exception | None = None) -> NoReturn:
    raise ServerErrorException(request=request, errors=_error_details(message, cause)) from cause


def raise_not_found(request: Request, message: str) -> NoReturn:
    raise NotFoundException(request=request, errors=_error_details(message))


def raise_authentication_error(
    request: Request,
    message: str,
    cause: Exception | None = None,
    *,
    status_code: int = status.HTTP_401_UNAUTHORIZED,
    headers: dict[str, str] | None = None,
) -> NoReturn:
    exception_class = (
        ForbiddenException if status_code == status.HTTP_403_FORBIDDEN else UnauthorizedException
    )
    raise exception_class(
        request=request, errors=_error_details(message, cause), headers=headers
    ) from cause


def _format_traceback_with_request_id(exc: Exception, request_id: str) -> str:
    """Prefix each traceback line so line-based filters keep the full stacktrace."""
    traceback_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    prefix = f"request_id={request_id} | "
    return "\n".join(f"{prefix}{line}" for line in traceback_text.splitlines())


def add_request_error_details(request: Request, exc: ProblemRegistryException) -> None:
    """Append supplied path values and consumed headers without duplicating details."""
    details = [
        registry.ErrorDetail(parameter=name, detail=str(value)[:4096])
        for name, value in request.path_params.items()
    ]
    header_names = {"X-Correlation-ID"}
    route = request.scope.get("route")
    dependant = getattr(route, "dependant", None)
    dependencies = [dependant] if dependant is not None else []
    while dependencies:
        dependency = dependencies.pop()
        header_names.update(field.alias for field in dependency.header_params)
        if isinstance(dependency.call, OAuth2):
            header_names.add("Authorization")
        dependencies.extend(dependency.dependencies)

    for name in sorted(header_names):
        for value in request.headers.getlist(name):
            details.append(registry.ErrorDetail(
                header=name,
                detail="[redacted]" if name.lower() == "authorization" else value[:4096],
            ))

    content = json.loads(exc.detail)
    errors = content.get("errors", [])
    for detail in details:
        item = detail.model_dump(mode="json", exclude_none=True)
        if item not in errors:
            errors.append(item)
    if errors:
        content["errors"] = errors[:1000]
    content.setdefault("request_id", correlation_id_ctx.get())
    exc.detail = json.dumps(content)


async def registry_exception_handler(request: Request, exc: ProblemRegistryException):
    """Add request context; let the registry handler build the response."""
    add_request_error_details(request, exc)
    logger.error(_format_traceback_with_request_id(exc, correlation_id_ctx.get()))
    return await problem_registry_exception_handler(request, exc)


async def generic_exception_handler(request: Request, exc: Exception):
    """Include the exception message in the registry ServerError response."""
    logger.error(_format_traceback_with_request_id(exc, correlation_id_ctx.get()))
    return await registry_exception_handler(
        request,
        ServerErrorException(
            request=request, errors=[registry.ErrorDetail(detail=str(exc)[:4096])]
        ),
    )


async def http_exception_handler(request: Request, exc: HTTPException):
    """Convert HTTP errors to registry problems, defaulting to ServerError (500)."""
    if isinstance(exc, ProblemRegistryException):
        return await registry_exception_handler(request, exc)

    exception_class = HTTP_PROBLEM_EXCEPTIONS.get(exc.status_code, ServerErrorException)
    return await registry_exception_handler(
        request,
        exception_class(
            request=request,
            errors=[registry.ErrorDetail(detail=str(exc.detail)[:4096])],
            headers=dict(exc.headers or {}),
        ),
    )


async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors: list[registry.ErrorDetail] = []
    for error in exc.errors():
        location = error.get("loc", ())
        detail = registry.ErrorDetail(detail=error["msg"][:4096])

        if location and location[0] == "body":
            detail.pointer = "/" + "/".join(
                str(part).replace("~", "~0").replace("/", "~1") for part in location[1:]
            ) if len(location) > 1 else ""
        elif location and location[0] == "header":
            detail.header = str(location[-1])
        elif location:
            detail.parameter = ".".join(str(part) for part in location[1:])

        errors.append(detail)
    return await registry_exception_handler(
        request,
        ValidationErrorException(request=request, errors=errors[:1000]),
    )


def register_exception_handlers(app):
    """Register RFC 9457 responses for domain, HTTP, validation and unexpected errors."""
    app.add_exception_handler(ProblemRegistryException, registry_exception_handler)
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, generic_exception_handler)
