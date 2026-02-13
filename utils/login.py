import getpass
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import requests
from dotenv import load_dotenv, set_key


def _to_bool(value: str, default: bool = True) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _join_url(base_url: str, endpoint: str) -> str:
    return f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"


def _extract_nested(data: Dict[str, Any], path: str) -> Optional[Any]:
    current: Any = data
    for key in path.split("."):
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _iter_token_paths(primary_path: str, fallback_paths: Optional[Iterable[str]] = None) -> Iterable[str]:
    yielded = set()
    for path in [primary_path, *(fallback_paths or [])]:
        cleaned = str(path).strip()
        if cleaned and cleaned not in yielded:
            yielded.add(cleaned)
            yield cleaned


@dataclass
class LoginConfig:
    api_base_url: str
    login_endpoint: str
    login_method: str = "POST"
    login_username_field: str = "email"
    login_password_field: str = "password"
    login_token_path: str = "data.token"
    api_timeout_seconds: int = 120
    api_verify_ssl: bool = True
    token_env_key: str = "TOKEN"
    env_file_path: str = ".env"

    @classmethod
    def from_env(cls, env_path: Optional[str] = None) -> "LoginConfig":
        load_dotenv(dotenv_path=env_path)
        return cls(
            api_base_url=os.getenv("API_BASE_URL", "https://example.com"),
            login_endpoint=os.getenv("LOGIN_ENDPOINT", "/v1/auth/login"),
            login_method=os.getenv("LOGIN_METHOD", "POST"),
            login_username_field=os.getenv("LOGIN_USERNAME_FIELD", "email"),
            login_password_field=os.getenv("LOGIN_PASSWORD_FIELD", "password"),
            login_token_path=os.getenv("LOGIN_TOKEN_PATH", "data.token"),
            api_timeout_seconds=int(os.getenv("API_TIMEOUT_SECONDS", "120")),
            api_verify_ssl=_to_bool(os.getenv("API_VERIFY_SSL", "true"), default=True),
            token_env_key=os.getenv("TOKEN_ENV_KEY", "TOKEN"),
            env_file_path=env_path or os.getenv("ENV_FILE_PATH", ".env"),
        )


def prompt_username(default_username: str = "") -> str:
    prompt_text = "Please enter your username/email"
    if default_username:
        prompt_text += f" [{default_username}]"
    prompt_text += ": "
    value = input(prompt_text).strip()
    return value or default_username


def prompt_password() -> str:
    return getpass.getpass("Please enter your password: ")


def login_with_credentials(config: LoginConfig, username: str, password: str) -> Dict[str, Any]:
    if not username:
        raise ValueError("Username/email is required.")
    if not password:
        raise ValueError("Password is required.")

    url = _join_url(config.api_base_url, config.login_endpoint)
    payload = {
        config.login_username_field: username,
        config.login_password_field: password,
    }

    response = requests.request(
        method=config.login_method.upper(),
        url=url,
        json=payload,
        timeout=config.api_timeout_seconds,
        verify=config.api_verify_ssl,
    )
    response.raise_for_status()
    return response.json()


def extract_token(login_response: Dict[str, Any], configured_path: str) -> Optional[str]:
    fallback_paths = ("token", "data.token", "data.authToken", "authToken", "access_token")
    for path in _iter_token_paths(configured_path, fallback_paths):
        value = _extract_nested(login_response, path)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def update_env_token(token: str, env_key: str = "TOKEN", env_path: str = ".env") -> None:
    env_file = Path(env_path)
    if not env_file.exists():
        env_file.parent.mkdir(parents=True, exist_ok=True)
        env_file.touch()
    set_key(str(env_file), env_key, token)


def interactive_login(
    *,
    env_path: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    allow_prompt: bool = True,
    write_token: bool = True,
    token_env_key: Optional[str] = None,
) -> Dict[str, Any]:
    config = LoginConfig.from_env(env_path=env_path)
    env_username = os.getenv("LOGIN_USERNAME", "") or os.getenv("LOGIN_EMAIL", "")
    env_password = os.getenv("LOGIN_PASSWORD", "")

    resolved_username = username or env_username
    resolved_password = password or env_password

    if allow_prompt and not resolved_username:
        resolved_username = prompt_username(default_username=env_username)
    if allow_prompt and not resolved_password:
        resolved_password = prompt_password()

    login_response = login_with_credentials(config, resolved_username, resolved_password)
    token = extract_token(login_response, config.login_token_path)
    if not token:
        raise ValueError(
            f"Login succeeded but token not found. Check LOGIN_TOKEN_PATH. "
            f"Top-level keys: {list(login_response.keys())}"
        )

    target_env_key = token_env_key or config.token_env_key
    if write_token:
        update_env_token(token, env_key=target_env_key, env_path=config.env_file_path)

    return {
        "token": token,
        "username": resolved_username,
        "env_key": target_env_key,
        "env_path": config.env_file_path,
        "response": login_response,
    }
