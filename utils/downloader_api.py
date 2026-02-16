import os
import subprocess
import sys
import time
from importlib.util import find_spec
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import httpx
from dotenv import load_dotenv


@dataclass
class APIConfig:
    base_url: str
    auth_token: str
    auth_header_name: str = "Authorization"
    auth_header_prefix: str = "Bearer"
    timeout_seconds: int = 120
    verify_ssl: bool = True
    request_log: bool = False

    projects_endpoint: str = "/v1/resources"
    plots_filter_endpoint: str = "/v1/resources/search"
    plots_details_endpoint: str = "/v1/resources/details"
    activities_filter_endpoint: str = "/v1/activities/search"
    activity_by_id_endpoint: str = "/v1/activities/{activity_id}"

    rows_key: str = "rows"
    total_pages_key: str = "totalPages"
    project_id_field: str = "id"
    project_name_field: str = "name"
    plot_id_field: str = "id"
    page_param_name: str = "page"
    page_in_body: bool = True
    first_page_number: int = 0

    @classmethod
    def from_env(cls) -> "APIConfig":
        load_dotenv()
        return cls(
            base_url=os.getenv("API_BASE_URL", "https://example.com"),
            auth_token=os.getenv("TOKEN", ""),
            auth_header_name=os.getenv("AUTH_HEADER_NAME", "Authorization"),
            auth_header_prefix=os.getenv("AUTH_HEADER_PREFIX", "Bearer"),
            timeout_seconds=int(os.getenv("API_TIMEOUT_SECONDS", "120")),
            verify_ssl=os.getenv("API_VERIFY_SSL", "true").lower() == "true",
            request_log=os.getenv("API_REQUEST_LOG", "false").lower() == "true",
            projects_endpoint=os.getenv("PROJECTS_ENDPOINT", "/v1/resources"),
            plots_filter_endpoint=os.getenv("PLOTS_FILTER_ENDPOINT", "/v1/resources/search"),
            plots_details_endpoint=os.getenv("PLOTS_DETAILS_ENDPOINT", "/v1/resources/details"),
            activities_filter_endpoint=os.getenv("ACTIVITIES_FILTER_ENDPOINT", "/v1/activities/search"),
            activity_by_id_endpoint=os.getenv("ACTIVITY_BY_ID_ENDPOINT", "/v1/activities/{activity_id}"),
            rows_key=os.getenv("API_ROWS_KEY", "rows"),
            total_pages_key=os.getenv("API_TOTAL_PAGES_KEY", "totalPages"),
            project_id_field=os.getenv("PROJECT_ID_FIELD", "id"),
            project_name_field=os.getenv("PROJECT_NAME_FIELD", "name"),
            plot_id_field=os.getenv("PLOT_ID_FIELD", "id"),
            page_param_name=os.getenv("API_PAGE_PARAM_NAME", "page"),
            page_in_body=os.getenv("API_PAGE_IN_BODY", "true").lower() == "true",
            first_page_number=int(os.getenv("API_FIRST_PAGE_NUMBER", "0")),
        )


class AsyncAPIClient:
    def __init__(self, config: APIConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None

    def _build_headers(self) -> Dict[str, str]:
        if not self.config.auth_token:
            return {}
        prefix = self.config.auth_header_prefix.strip()
        token_value = f"{prefix} {self.config.auth_token}".strip() if prefix else self.config.auth_token
        return {self.config.auth_header_name: token_value}

    def _url(self, endpoint: str) -> str:
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint
        return f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.config.timeout_seconds,
                verify=self.config.verify_ssl,
            )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def request(
        self,
        endpoint: str,
        method: str = "GET",
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        client = await self._get_client()
        url = self._url(endpoint)
        page_param_name = self.config.page_param_name
        page_value = None
        if isinstance(params, dict):
            page_value = params.get(page_param_name)
        if page_value is None and isinstance(json_body, dict):
            page_value = json_body.get(page_param_name)
        if page_value is None and isinstance(data, dict):
            page_value = data.get(page_param_name)
        if self.config.request_log:
            page_text = f" page={page_value}" if page_value is not None else ""
            print(f"[api] {method.upper()} {url}{page_text}")

        start = time.perf_counter()
        response = await client.request(
            method.upper(),
            url,
            params=params,
            json=json_body,
            data=data,
            headers=self._build_headers(),
        )
        if self.config.request_log:
            elapsed_ms = (time.perf_counter() - start) * 1000
            print(f"[api] <- {response.status_code} {method.upper()} {url} ({elapsed_ms:.0f} ms)")
        response.raise_for_status()
        return response.json()

    @staticmethod
    def save_json(data: Dict[str, Any], output_path: str) -> Path:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8") as f:
            import json

            json.dump(data, f, indent=2, ensure_ascii=False)
        return output


def is_colab() -> bool:
    return "google.colab" in sys.modules


def _run_command(cmd: str) -> None:
    print(f"$ {cmd}")
    subprocess.check_call(cmd, shell=True)


def _modules_available(*module_names: str) -> bool:
    return all(find_spec(module_name) is not None for module_name in module_names)


def ensure_runtime() -> Path:
    if is_colab():
        repo_url = "https://github.com/miqbalf/api_async_scrape.git"
        repo_dir = Path("/content/api_async_scrape")
        if not repo_dir.exists():
            _run_command(f"git clone {repo_url} {repo_dir}")
        os.chdir(repo_dir)
        _run_command("python -m pip install -q --upgrade pip")
        # Minimal deps for Colab; full requirements_linux.txt often conflicts with Colab's env
        _run_command(
            "python -m pip install -q httpx requests python-dotenv aiofiles pandas geopandas"
        )
        return repo_dir

    force_install = os.getenv("API_ASYNC_SCRAPE_FORCE_INSTALL", "false").lower() == "true"
    required_modules = ("httpx", "requests", "dotenv", "aiofiles", "pandas", "geopandas")
    if force_install or not _modules_available(*required_modules):
        _run_command("python -m pip install -q --upgrade pip")
        _run_command("python -m pip install -q httpx requests python-dotenv aiofiles pandas geopandas")
    else:
        print("Runtime dependencies already available; skipping pip install.")
    return Path.cwd()


def build_client_from_env() -> Tuple[APIConfig, AsyncAPIClient]:
    load_dotenv()
    cfg = APIConfig.from_env()
    return cfg, AsyncAPIClient(cfg)


class Request:
    """
    Backward-compatible request wrapper used by existing modules.
    """

    def __init__(
        self,
        urlname: str,
        auth_token: str,
        load_url: str = "post",
        payload_data: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs,
    ):
        self.urlname = urlname
        self.auth_token = auth_token
        self.load_url = load_url.lower()
        self.payload_data = dict(payload_data or {})
        self.p_args = args
        self.k_args = kwargs

    def _headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {self.auth_token}",
        }

    async def request_res(self, page_input: int = 0):
        params = dict(self.k_args)
        payload = dict(self.payload_data)
        if page_input >= 0:
            if self.load_url in {"post", "patch_api"}:
                payload.setdefault("page", page_input)
            else:
                params.setdefault("page", page_input)

        async with httpx.AsyncClient(timeout=1200) as session:
            if self.load_url in {"get", "downloadgeojsonplot"}:
                request_page = await session.get(self.urlname, headers=self._headers(), params=params)
            elif self.load_url == "post":
                request_page = await session.post(self.urlname, headers=self._headers(), data=payload)
            elif self.load_url == "patch_api":
                merged_patch_payload: Dict[str, Any] = {}
                for arg in self.p_args:
                    if isinstance(arg, dict):
                        merged_patch_payload.update(arg)
                plot_id = self.k_args.get("plotId")
                patch_url = f"{self.urlname}{plot_id}" if plot_id is not None else self.urlname
                request_page = await session.patch(patch_url, headers=self._headers(), json=merged_patch_payload)
            else:
                raise ValueError(f"Unsupported load_url: {self.load_url}")

            print(request_page.status_code, " is the request status")
            return request_page