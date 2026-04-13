import os

from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    database_url_sync: str
    debug: bool = False
    data_dir: str = "/tmp/bellona_uploads"
    claude_api_key: str = ""
    claude_model: str = "claude-sonnet-4-6"

    model_config = {
        "env_file": os.getenv("ENV_FILE", ".env"),
        "env_file_encoding": "utf-8",
    }


@lru_cache
def get_settings() -> Settings:
    return Settings()
