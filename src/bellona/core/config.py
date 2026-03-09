from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://bellona:bellona@localhost:5432/bellona"
    database_url_sync: str = "postgresql+psycopg2://bellona:bellona@localhost:5432/bellona"
    debug: bool = False

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
