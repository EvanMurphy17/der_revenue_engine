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
    """
    Return the repository root path.

    Resolution order:
      1) DRE_PROJECT_ROOT env var, if set
      2) Walk up from this file looking for a directory that has pyproject.toml or .git
      3) Fallback to two parents up from this file (repo layout: <repo>/dre/config.py)
    """
    # 1) explicit override
    env = os.getenv("DRE_PROJECT_ROOT")
    if env:
        try:
            p = Path(env).expanduser().resolve()
            if p.exists():
                return p
        except Exception:
            pass

    # 2) search upward for repo markers
    here = Path(__file__).resolve()
    for cand in [here, *here.parents]:
        if (cand / "pyproject.toml").exists() or (cand / ".git").exists():
            return cand

    # 3) fallback
    return here.parents[1]
