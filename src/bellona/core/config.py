from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    database_url_sync: str
    debug: bool

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
