import pathlib

import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    claude_model: str = "claude-sonnet-4-6"
    google_form_url: str = ""
    google_spreadsheet_id: str = ""
    google_service_account_json: str = "./config/service_account.json"
    github_token: str = ""
    github_repo: str = ""
    databricks_host: str = ""
    databricks_token: str = ""
    slack_webhook_url: str = ""


def _load_yaml() -> dict:
    path = pathlib.Path(__file__).parent / "settings.yaml"
    with path.open() as f:
        return yaml.safe_load(f) or {}


settings = Settings()
yaml_config = _load_yaml()
