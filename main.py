from predict2.app import app
from predict2.action_api import router as action_router

app.include_router(action_router)

__all__ = ["app"]
