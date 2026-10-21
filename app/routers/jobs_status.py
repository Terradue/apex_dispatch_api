from app.middleware.error_handling import raise_server_error
from fastapi import status
from app.middleware.request_context import request_context
import asyncio
import json
from typing import List

from http_apis_problems_detail.fastapi import PROBLEM_RESPONSES, ProblemRegistryException
from fastapi import (
    Request, HTTPException, APIRouter, Depends, Query, WebSocket, WebSocketDisconnect,
)
from sqlalchemy.orm import Session
from loguru import logger

from app.middleware.error_handling import add_request_error_details
from app.database.db import SessionLocal, get_db
from app.schemas.jobs_status import JobsFilter, JobsStatusResponse
from app.schemas.websockets import WSStatusMessage
from app.services.processing import get_processing_jobs_by_user_id
from app.services.upscaling import get_upscaling_tasks_by_user_id
from app.auth import oauth2_scheme, websocket_authenticate

router = APIRouter()

DEFAULT_FILTERS = [JobsFilter.upscaling, JobsFilter.processing]


@router.get(
    "/jobs_status",
    tags=["Upscale Tasks", "Unit Jobs"],
    summary="Get a list of all upscaling tasks & processing jobs for the authenticated user",
    responses={
        status.HTTP_500_INTERNAL_SERVER_ERROR: PROBLEM_RESPONSES[
            status.HTTP_500_INTERNAL_SERVER_ERROR
        ],
    },
)
async def get_jobs_status(
    request: Request,
    db: Session = Depends(get_db),
    token: str = Depends(oauth2_scheme),
    filter: List[JobsFilter] = Query(
        DEFAULT_FILTERS,
        description="Filter jobs: upscaling, processing. Can be provided multiple times.",
    ),
) -> JobsStatusResponse:
    """
    Return combined list of upscaling tasks and processing jobs for the authenticated user.
    """
    try:
        logger.debug("Fetching jobs list")
        upscaling_tasks = (
            await get_upscaling_tasks_by_user_id(token, db)
            if JobsFilter.upscaling in filter
            else []
        )
        processing_jobs = (
            await get_processing_jobs_by_user_id(token, db)
            if JobsFilter.processing in filter
            else []
        )
        return JobsStatusResponse(
            upscaling_tasks=upscaling_tasks,
            processing_jobs=processing_jobs,
        )
    except HTTPException as de:
        raise de
    except Exception as e:
        logger.error(f"Error retrieving job status: {e}")
        raise_server_error(
            request,
            'An error occurred while retrieving the job status.',
            e,
        )


@router.websocket(
    "/ws/jobs_status",
)
async def ws_jobs_status(
    websocket: WebSocket,
    interval: int = 10,
    filter: List[JobsFilter] = Query(DEFAULT_FILTERS),
):
    """
    Return combined list of upscaling tasks and processing jobs for the authenticated user.
    """

    token = await websocket_authenticate(websocket)
    if not token:
        return

    await websocket.send_json(
        WSStatusMessage(type="init", message="Starting status stream").model_dump()
    )

    try:
        while True:
            with SessionLocal() as db:
                await websocket.send_json(
                    WSStatusMessage(
                        type="loading",
                        message="Starting retrieval of status",
                    ).model_dump()
                )
                status = await get_jobs_status(request_context.get(), db, token, filter=filter)
                await websocket.send_json(
                    WSStatusMessage(
                        type="status",
                        data=json.loads(status.model_dump_json()),
                    ).model_dump()
                )
                await asyncio.sleep(interval)

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except HTTPException as ae:
        if isinstance(ae, ProblemRegistryException):
            add_request_error_details(Request({**websocket.scope, "type": "http"}), ae)
        logger.error(f"HTTP exception detected: {ae.detail}")
        await websocket.send_json(
            WSStatusMessage(type="error", message=ae.detail).model_dump()
        )
        await websocket.close(code=1011, reason=str(ae.status_code))
    except Exception as e:
        logger.error(f"Unexpected error occurred during websocket : {e}")
        await websocket.send_json(
            WSStatusMessage(
                type="error",
                message="An error occurred while monitoring the job status.",
            ).model_dump()
        )
        await websocket.close(code=1011, reason="INTERNAL_ERROR")
    finally:
        db.close()
