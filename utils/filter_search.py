import json
from typing import Any, Dict, List, Optional

from .downloader_api import APIConfig, AsyncAPIClient
from .list_all_files import list_files


def filter_rows_by_keyword(
    rows: List[Dict[str, Any]],
    keyword: str,
    *,
    id_field: str = "id",
    name_field: str = "name",
) -> List[Dict[str, Any]]:
    key = str(keyword).strip().lower()
    if not key:
        return rows
    return [
        row
        for row in rows
        if key == str(row.get(id_field, "")).lower()
        or key in str(row.get(name_field, "")).lower()
    ]


async def search_projects(client: AsyncAPIClient, cfg: APIConfig, keyword: str = ""):
    data = await client.request(cfg.projects_endpoint, method="GET")
    rows = list(data.get(cfg.rows_key, []))
    return filter_rows_by_keyword(
        rows,
        keyword=keyword,
        id_field=cfg.project_id_field,
        name_field=cfg.project_name_field,
    )


class FilterSearch:
    """
    Backward-compatible project search helper.
    Works with local JSON files and non-interactive filtering.
    """

    def __init__(
        self,
        input_name,
        end_point,
        auth_token,
        directory_path="./json_downloaded_api/proj_ID",
        rows_key: str = "rows",
        project_id_field: str = "id",
        project_name_field: str = "name",
    ):
        self.input_name = input_name
        self.end_point = end_point
        self.auth_token = auth_token
        self.directory_path = directory_path
        self.rows_key = rows_key
        self.project_id_field = project_id_field
        self.project_name_field = project_name_field
        self.file_loc = self.get_updated_file()

    def get_updated_file(self) -> str:
        files = sorted(list_files(self.directory_path))
        if not files:
            raise FileNotFoundError(f"No files found in {self.directory_path}")
        return files[-1]

    def _load_rows(self) -> List[Dict[str, Any]]:
        with open(self.file_loc, "r", encoding="utf-8") as file:
            data = json.load(file)
        if data is None:
            raise ValueError("Invalid JSON data.")
        return list(data.get(self.rows_key, []))

    def search_proj(self):
        rows = self._load_rows()
        matches = filter_rows_by_keyword(
            rows,
            keyword=str(self.input_name),
            id_field=self.project_id_field,
            name_field=self.project_name_field,
        )
        return [
            {
                "newName": str(d.get(self.project_name_field, "")).upper(),
                "id": d.get(self.project_id_field),
            }
            for d in matches
        ]

    def repeat_search(self):
        self.input_name = input("Please input another search keyword of Project or projectID number: ")
        return self.search_proj()

    def search_loop(self, prevList):
        # Preserve old behavior but avoid network-refresh side effects.
        while prevList == []:
            try:
                self.file_loc = self.get_updated_file()
                prevList = self.repeat_search()
            except KeyboardInterrupt:
                print("\nProgram interrupted.")
                break
        return prevList