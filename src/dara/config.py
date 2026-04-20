import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    anthropic_api_key: str = field(default_factory=lambda: os.environ["ANTHROPIC_API_KEY"])
    anthropic_model: str = field(default_factory=lambda: os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6"))

    airflow_base_url: str = field(default_factory=lambda: os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080"))
    airflow_username: str = field(default_factory=lambda: os.getenv("AIRFLOW_USERNAME", "admin"))
    airflow_password: str = field(default_factory=lambda: os.getenv("AIRFLOW_PASSWORD", "admin"))

    bigquery_project: str = field(default_factory=lambda: os.getenv("BIGQUERY_PROJECT", ""))
    google_credentials_path: str = field(default_factory=lambda: os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""))

    dbt_manifest_path: str = field(default_factory=lambda: os.getenv("DBT_MANIFEST_PATH", "./target/manifest.json"))

    webhook_port: int = field(default_factory=lambda: int(os.getenv("DARA_WEBHOOK_PORT", "9000")))
    webhook_secret: str = field(default_factory=lambda: os.getenv("DARA_WEBHOOK_SECRET", ""))


settings = Settings()
