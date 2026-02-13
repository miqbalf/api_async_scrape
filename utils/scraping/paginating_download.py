import asyncio
import json
import os
from typing import Dict, List, Optional

import aiofiles

from ..downloader_api import APIConfig, AsyncAPIClient, Request


class PaginatingDownload(Request):
    def __init__(self, proj_id, urlname, auth_token, request_type="post", *args, **kwargs):
        self.proj_id = proj_id
        self.request_type = request_type
        resource_id_field = os.getenv("RESOURCE_ID_PAYLOAD_FIELD", os.getenv("PROJECT_ID_PAYLOAD_FIELD", "resourceId"))
        self.payload_data = {resource_id_field: self.proj_id}
        super().__init__(urlname, auth_token, load_url=self.request_type, payload_data=self.payload_data)
        self.p_args = args
        self.k_args = kwargs

    async def total_pages(self):
        res = await self.request_res()
        data = res.json()
        total_pages = int(data.get("totalPages", 0) or 0)
        print(f"downloading the files for {total_pages} page(s) -- if available and have permission")
        return total_pages

    async def download_all_pages(self, total_page, file_json_output):
        tasks = [self.request_res(page_input=i) for i in range(total_page)]
        row_json = {
            "rows": [
                row
                for response in await asyncio.gather(*tasks)
                for row in response.json().get("rows", [])
            ]
        }
        with open(file_json_output, "w", encoding="utf-8") as output_file:
            json.dump(row_json, output_file)
        return row_json

    async def dumping_json_geojson_get(self, file_json_output):
        response = await self.request_res()
        data = response.json()
        row_json = {"rows": data}

        async with aiofiles.open(file_json_output, "w", encoding="utf-8") as output_file:
            await output_file.write(json.dumps(row_json, ensure_ascii=False, indent=2))

        return row_json


async def request_paginated_rows(
    client: AsyncAPIClient,
    cfg: APIConfig,
    endpoint: str,
    *,
    method: str = "POST",
    base_payload: Optional[Dict] = None,
    extra_params: Optional[Dict] = None,
    max_pages: Optional[int] = None,
) -> Dict:
    payload = dict(base_payload or {})
    params = dict(extra_params or {})

    first_payload = dict(payload)
    first_params = dict(params)
    if cfg.page_in_body:
        first_payload[cfg.page_param_name] = cfg.first_page_number
    else:
        first_params[cfg.page_param_name] = cfg.first_page_number

    first_data = await client.request(
        endpoint,
        method=method,
        params=first_params if method.upper() == "GET" or not cfg.page_in_body else None,
        json_body=first_payload if method.upper() != "GET" else None,
        data=first_payload if method.upper() == "POST" else None,
    )

    rows_key = cfg.rows_key
    total_pages = int(first_data.get(cfg.total_pages_key, 1) or 1)
    if max_pages is not None:
        total_pages = min(total_pages, max_pages)

    if total_pages <= 1:
        return {rows_key: list(first_data.get(rows_key, []))}

    async def _fetch(page_number: int) -> List[Dict]:
        page_payload = dict(payload)
        page_params = dict(params)
        if cfg.page_in_body:
            page_payload[cfg.page_param_name] = page_number
        else:
            page_params[cfg.page_param_name] = page_number
        page_data = await client.request(
            endpoint,
            method=method,
            params=page_params if method.upper() == "GET" or not cfg.page_in_body else None,
            json_body=page_payload if method.upper() != "GET" else None,
            data=page_payload if method.upper() == "POST" else None,
        )
        return list(page_data.get(rows_key, []))

    tasks = [
        _fetch(page)
        for page in range(cfg.first_page_number, cfg.first_page_number + total_pages)
    ]
    all_rows_nested = await asyncio.gather(*tasks)
    all_rows = [item for page_rows in all_rows_nested for item in page_rows]
    return {rows_key: all_rows}


async def download_plots(
    client: AsyncAPIClient,
    cfg: APIConfig,
    project_id: int,
    output_path: str,
    extra_filters: Optional[Dict] = None,
):
    payload_field = os.getenv("RESOURCE_ID_PAYLOAD_FIELD", os.getenv("PROJECT_ID_PAYLOAD_FIELD", "resourceId"))
    payload = {payload_field: project_id}
    if extra_filters:
        payload.update(extra_filters)

    data = await request_paginated_rows(
        client,
        cfg,
        cfg.plots_filter_endpoint,
        method="POST",
        base_payload=payload,
    )
    output = client.save_json(data, output_path)
    return data, output