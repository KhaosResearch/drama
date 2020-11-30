import uvicorn
from fastapi import Depends, FastAPI
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.models import APIKey
from fastapi.openapi.utils import get_openapi
from starlette import status
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse, Response

from drama import __version__
from drama.api.routes.workflow import router
from drama.api.security import get_api_key
from drama.config import settings
from drama.database import close_db_connection, create_db_connection
from drama.logger import get_logger

app = FastAPI(
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    root_path=settings.ROOT_PATH,
)


# events

app.add_event_handler("startup", create_db_connection)
app.add_event_handler("shutdown", close_db_connection)


# cors settings

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# api routes


@app.get("/api/health", name="Health check", status_code=status.HTTP_200_OK, tags=["health"])
async def health():
    return Response(status_code=status.HTTP_200_OK)


app.include_router(router, prefix="/api/v2/workflow")


# swagger documentation


@app.get("/api/openapi.json", tags=["documentation"])
async def get_open_api_endpoint(api_key: APIKey = Depends(get_api_key)):
    response = JSONResponse(
        get_openapi(
            title="DRAMA",
            version=__version__,
            description="https://github.com/benhid/drama",
            routes=app.routes,
        )
    )
    return response


@app.get("/api/docs", tags=["documentation"])
async def get_documentation(api_key: APIKey = Depends(get_api_key)):
    response = get_swagger_ui_html(
        openapi_url=f"/api/openapi.json?{settings.API_KEY_NAME}={settings.API_KEY}",
        title="Documentation",
    )
    return response


def run_server():
    logger = get_logger(__name__)
    logger.info(f"🚀 Deploying server at http://{settings.API_HOST}:{settings.API_PORT}")

    uvicorn.run(
        app,
        host=settings.API_HOST,
        port=settings.API_PORT,
        root_path=settings.ROOT_PATH,
        log_level="trace" if settings.API_DEBUG else "info",
    )
