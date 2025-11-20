from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ENV: str = "dev"
    SILVER_PATH: str = "s3://datalake-iot-smart-building/silver/"
    SILVER_MAX_LIMIT: int = 1000

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    return Settings()
