import asyncio
import json
import os
from typing import Any, Dict, List, Optional, Sequence

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

    @staticmethod
    async def request_paginated_rows(
        client: AsyncAPIClient,
        cfg: APIConfig,
        endpoint: str,
        *,
        method: str = "POST",
        base_payload: Optional[Dict] = None,
        extra_params: Optional[Dict] = None,
        max_pages: Optional[int] = None,
        page_param_name: Optional[str] = None,
        first_page_number: Optional[int] = None,
        page_in_body: Optional[bool] = None,
        total_pages_key: Optional[str] = None,
    ) -> Dict:
        resolved_method = method.upper()
        resolved_page_param_name = page_param_name or cfg.page_param_name
        resolved_first_page_number = cfg.first_page_number if first_page_number is None else first_page_number
        resolved_page_in_body = cfg.page_in_body if page_in_body is None else page_in_body
        if resolved_method == "GET":
            resolved_page_in_body = False

        payload = dict(base_payload or {})
        params = dict(extra_params or {})

        first_payload = dict(payload)
        first_params = dict(params)
        if resolved_method == "GET":
            first_params.update(first_payload)
            first_payload = {}

        if resolved_page_in_body:
            first_payload[resolved_page_param_name] = resolved_first_page_number
        else:
            first_params[resolved_page_param_name] = resolved_first_page_number

        first_data = await client.request(
            endpoint,
            method=resolved_method,
            params=first_params if resolved_method == "GET" or not resolved_page_in_body else None,
            json_body=first_payload if resolved_method != "GET" else None,
        )

        rows_key = cfg.rows_key
        resolved_total_pages_key = total_pages_key or cfg.total_pages_key
        total_pages = int(first_data.get(resolved_total_pages_key, 1) or 1)
        first_rows = list(first_data.get(rows_key, []))
        if max_pages is not None:
            total_pages = min(total_pages, max_pages)

        if total_pages <= 1:
            return {rows_key: first_rows}

        async def _fetch(page_number: int) -> List[Dict]:
            page_payload = dict(payload)
            page_params = dict(params)
            if resolved_method == "GET":
                page_params.update(page_payload)
                page_payload = {}

            if resolved_page_in_body:
                page_payload[resolved_page_param_name] = page_number
            else:
                page_params[resolved_page_param_name] = page_number
            page_data = await client.request(
                endpoint,
                method=resolved_method,
                params=page_params if resolved_method == "GET" or not resolved_page_in_body else None,
                json_body=page_payload if resolved_method != "GET" else None,
            )
            return list(page_data.get(rows_key, []))

        tasks = [
            _fetch(page)
            for page in range(resolved_first_page_number + 1, resolved_first_page_number + total_pages)
        ]
        all_rows_nested = await asyncio.gather(*tasks)
        all_rows = list(first_rows)
        all_rows.extend(item for page_rows in all_rows_nested for item in page_rows)
        return {rows_key: all_rows}

    @staticmethod
    def _get_dotted_value(source: Any, dotted_path: str) -> Any:
        current = source
        for part in dotted_path.split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current

    @staticmethod
    def _resolve_target_records(payload: Dict[str, Any], path: Sequence[str]) -> List[Dict[str, Any]]:
        current_items: List[Any] = [payload]
        for key in path:
            next_items: List[Any] = []
            for item in current_items:
                if isinstance(item, dict):
                    value = item.get(key)
                    if isinstance(value, list):
                        next_items.extend(value)
                    elif value is not None:
                        next_items.append(value)
                elif isinstance(item, list):
                    for sub in item:
                        if isinstance(sub, dict):
                            value = sub.get(key)
                            if isinstance(value, list):
                                next_items.extend(value)
                            elif value is not None:
                                next_items.append(value)
            current_items = next_items
        return [item for item in current_items if isinstance(item, dict)]

    @classmethod
    async def _fetch_injection_rows(
        cls,
        client: AsyncAPIClient,
        cfg: APIConfig,
        source: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        # Allow passing pre-fetched rows to avoid extra request.
        if isinstance(source.get("rows"), list):
            return [r for r in source["rows"] if isinstance(r, dict)]

        endpoint = source.get("endpoint")
        if not endpoint:
            return []

        source_method = str(source.get("method", "GET")).upper()
        source_rows_key = str(source.get("rows_key", cfg.rows_key))
        source_page_param_name = source.get("page_param_name")
        source_first_page_number = source.get("first_page_number")
        source_page_in_body = source.get("page_in_body")
        source_total_pages_key = source.get("total_pages_key")
        source_max_pages = source.get("max_pages")

        source_payload = dict(source.get("root_payload") or {})
        source_params = dict(source.get("root_params") or {})
        source_filters = source.get("filters") or {}
        if isinstance(source_filters, dict):
            cleaned = {k: v for k, v in source_filters.items() if v is not None}
            source_payload.update(cleaned)
            source_params.update(cleaned)

        source_data = await cls.request_paginated_rows(
            client,
            cfg,
            endpoint,
            method=source_method,
            base_payload=source_payload,
            extra_params=source_params,
            max_pages=source_max_pages,
            page_param_name=source_page_param_name,
            first_page_number=source_first_page_number,
            page_in_body=source_page_in_body,
            total_pages_key=source_total_pages_key,
        )
        return [r for r in source_data.get(source_rows_key, []) if isinstance(r, dict)]

    @classmethod
    async def download_records(
        cls,
        client: AsyncAPIClient,
        cfg: APIConfig,
        endpoint: str,
        output_path: str,
        *,
        root_payload: Optional[Dict[str, Any]] = None,
        root_params: Optional[Dict[str, Any]] = None,
        extra_filters: Optional[Dict[str, Any]] = None,
        filter_payload: Optional[Dict[str, Any]] = None,
        filter_method: str = "POST",
        page_param_name: Optional[str] = None,
        first_page_number: Optional[int] = None,
        page_in_body: Optional[bool] = None,
        total_pages_key: Optional[str] = None,
        fetch_details: bool = True,
        details_endpoint: Optional[str] = None,
        details_id_field: Optional[str] = None,
        details_ids_param: Optional[str] = None,
        details_batch_size: Optional[int] = None,
        details_concurrency: Optional[int] = None,
        details_id_in_path: Optional[bool] = None,
        details_id_placeholder: str = "{id}",
        details_method: str = "GET",
        details_payload: Optional[Dict[str, Any]] = None,
        details_ids_as_list: bool = False,
        details_ids_key: Optional[str] = None,
        details_max_ids: Optional[int] = None,
        inject_sources: Optional[List[Dict[str, Any]]] = None,
        target_records_path: Optional[Sequence[str]] = None,
        **filter_kwargs: Any,
    ):
        payload: Dict[str, Any] = dict(root_payload or {})
        params: Dict[str, Any] = dict(root_params or {})
        for filter_source in (extra_filters, filter_payload, filter_kwargs):
            if filter_source:
                cleaned = {k: v for k, v in filter_source.items() if v is not None}
                payload.update(cleaned)
                params.update(cleaned)

        data = await cls.request_paginated_rows(
            client,
            cfg,
            endpoint,
            method=filter_method,
            base_payload=payload,
            extra_params=params,
            page_param_name=page_param_name,
            first_page_number=first_page_number,
            page_in_body=page_in_body,
            total_pages_key=total_pages_key,
        )

        final_payload: Dict[str, Any]
        if not fetch_details:
            final_payload = data
        else:
            rows_key = cfg.rows_key
            id_field = details_id_field or cfg.plot_id_field or os.getenv("PLOT_ID_FIELD", "id")
            ids_param = details_ids_param or os.getenv("PLOTS_DETAILS_IDS_PARAM", "ids")
            ids_key = details_ids_key or ids_param
            batch_size = details_batch_size if details_batch_size is not None else int(os.getenv("PLOTS_DETAILS_BATCH_SIZE", "200"))
            concurrency = details_concurrency if details_concurrency is not None else int(os.getenv("PLOTS_DETAILS_CONCURRENCY", "8"))
            resolved_details_endpoint = details_endpoint
            if not resolved_details_endpoint:
                final_payload = data
            else:
                if batch_size <= 0:
                    batch_size = 200
                if concurrency <= 0:
                    concurrency = 8

                filtered_rows = list(data.get(rows_key, []))
                record_ids = [
                    row.get(id_field)
                    for row in filtered_rows
                    if isinstance(row, dict) and row.get(id_field) is not None
                ]
                # Keep order but remove duplicates
                record_ids = list(dict.fromkeys(record_ids))
                if details_max_ids is not None and details_max_ids > 0:
                    record_ids = record_ids[:details_max_ids]

                if not record_ids:
                    final_payload = {rows_key: []}
                else:
                    def _normalize_detail_rows(response_data: Any) -> List[Dict]:
                        if isinstance(response_data, dict):
                            if isinstance(response_data.get(rows_key), list):
                                return [r for r in response_data[rows_key] if isinstance(r, dict)]
                            if isinstance(response_data.get("rows"), list):
                                return [r for r in response_data["rows"] if isinstance(r, dict)]
                            return [response_data]
                        if isinstance(response_data, list):
                            return [r for r in response_data if isinstance(r, dict)]
                        return []

                    if details_id_in_path is None:
                        details_id_in_path = details_id_placeholder in resolved_details_endpoint

                    semaphore = asyncio.Semaphore(concurrency)

                    async def _fetch_detail_single(single_id: Any) -> List[Dict]:
                        async with semaphore:
                            detail_endpoint = resolved_details_endpoint.replace(details_id_placeholder, str(single_id))
                            detail_data = await client.request(detail_endpoint, method=details_method.upper())
                            return _normalize_detail_rows(detail_data)

                    async def _fetch_detail_batch(batch_ids: List[Any]) -> List[Dict]:
                        async with semaphore:
                            ids_csv = ",".join(str(i) for i in batch_ids)
                            method_upper = details_method.upper()
                            query_params = {ids_param: ids_csv} if method_upper == "GET" else None
                            body_payload = dict(details_payload or {})
                            if method_upper != "GET":
                                body_payload[ids_key] = list(batch_ids) if details_ids_as_list else ids_csv
                            detail_data = await client.request(
                                resolved_details_endpoint,
                                method=method_upper,
                                params=query_params,
                                json_body=body_payload if method_upper != "GET" else None,
                            )
                            return _normalize_detail_rows(detail_data)

                    if details_id_in_path:
                        detail_rows_nested = await asyncio.gather(*[_fetch_detail_single(i) for i in record_ids])
                    else:
                        id_batches = [
                            record_ids[start : start + batch_size]
                            for start in range(0, len(record_ids), batch_size)
                        ]
                        detail_rows_nested = await asyncio.gather(*[_fetch_detail_batch(b) for b in id_batches])
                    detail_rows = [row for batch in detail_rows_nested for row in batch]
                    final_payload = {rows_key: detail_rows}

        # Optional idempotent injection: attaches source context by key into target records.
        # - No source/no match => no mutation.
        # - Existing attach key is overwritten with latest value (idempotent behavior).
        if inject_sources:
            target_path = list(target_records_path) if target_records_path else [cfg.rows_key]
            target_records = cls._resolve_target_records(final_payload, target_path)
            for source_cfg in inject_sources:
                attach_as = str(source_cfg.get("attach_as", "context"))
                target_key = str(source_cfg.get("target_key", "id"))
                source_key = str(source_cfg.get("source_key", "id"))
                source_rows = await cls._fetch_injection_rows(client, cfg, source_cfg)
                source_lookup = {
                    cls._get_dotted_value(src_row, source_key): src_row
                    for src_row in source_rows
                    if isinstance(src_row, dict) and cls._get_dotted_value(src_row, source_key) is not None
                }
                if not source_lookup:
                    continue
                for target_row in target_records:
                    match_value = cls._get_dotted_value(target_row, target_key)
                    if match_value is None:
                        continue
                    matched = source_lookup.get(match_value)
                    if matched is None:
                        continue
                    target_row[attach_as] = matched

        output = client.save_json(final_payload, output_path)
        return final_payload, output

    @classmethod
    async def download_plots(
        cls,
        client: AsyncAPIClient,
        cfg: APIConfig,
        project_id: int,
        output_path: str,
        extra_filters: Optional[Dict[str, Any]] = None,
        filter_payload: Optional[Dict[str, Any]] = None,
        project_id_payload_field: Optional[str] = None,
        fetch_details: bool = True,
        **filter_kwargs: Any,
    ):
        payload_field = (
            project_id_payload_field
            or os.getenv("PROJECT_ID_PAYLOAD_FIELD")
            or os.getenv("RESOURCE_ID_PAYLOAD_FIELD")
            or "resourceId"
        )
        root_payload = {payload_field: project_id}
        return await cls.download_records(
            client,
            cfg,
            endpoint=cfg.plots_filter_endpoint,
            output_path=output_path,
            root_payload=root_payload,
            extra_filters=extra_filters,
            filter_payload=filter_payload,
            fetch_details=fetch_details,
            details_endpoint=cfg.plots_details_endpoint,
            details_id_field=cfg.plot_id_field,
            **filter_kwargs,
        )