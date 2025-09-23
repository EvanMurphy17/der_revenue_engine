import os
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()


class Settings(BaseModel):
    app_env: str = os.getenv("APP_ENV", "dev")
    gridstatus_api_key: str | None = os.getenv("GRIDSTATUS_API_KEY")
    dsire_token: str | None = os.getenv("DSIRE_TOKEN")


settings = Settings()


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]
