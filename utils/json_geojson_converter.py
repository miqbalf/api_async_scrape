import json
from typing import Any, Dict, List, Optional, Sequence, Tuple

import geopandas as gpd


class JsonGeoJSON:
    def __init__(self, input_json="../json_downloaded_api/plots/test.json", input_dict=None):
        self.input_json = input_json
        self.input_dict = input_dict

    def input_json_convert(self, rows_key: str = "rows") -> List[Dict[str, Any]]:
        with open(self.input_json, "r", encoding="utf-8") as file:
            input_data = json.load(file)
        if input_data is None:
            raise ValueError("Invalid JSON data.")
        return list(input_data.get(rows_key, []))

    @staticmethod
    def _resolve_records(input_data: Any, records_path: Sequence[str]) -> List[Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]]:
        current_items: List[Tuple[Any, Dict[str, Dict[str, Any]]]] = [(input_data, {})]
        for key in records_path:
            next_items: List[Tuple[Any, Dict[str, Dict[str, Any]]]] = []
            for item, trail in current_items:
                if isinstance(item, dict):
                    value = item.get(key)
                    if isinstance(value, list):
                        for sub_item in value:
                            if isinstance(sub_item, dict):
                                next_trail = dict(trail)
                                next_trail[key] = sub_item
                                next_items.append((sub_item, next_trail))
                            else:
                                next_items.append((sub_item, dict(trail)))
                    elif value is not None:
                        if isinstance(value, dict):
                            next_trail = dict(trail)
                            next_trail[key] = value
                            next_items.append((value, next_trail))
                        else:
                            next_items.append((value, dict(trail)))
                elif isinstance(item, list):
                    for sub_item in item:
                        if isinstance(sub_item, dict):
                            value = sub_item.get(key)
                            if isinstance(value, list):
                                for inner_item in value:
                                    if isinstance(inner_item, dict):
                                        next_trail = dict(trail)
                                        next_trail[key] = inner_item
                                        next_items.append((inner_item, next_trail))
                                    else:
                                        next_items.append((inner_item, dict(trail)))
                            elif value is not None:
                                if isinstance(value, dict):
                                    next_trail = dict(trail)
                                    next_trail[key] = value
                                    next_items.append((value, next_trail))
                                else:
                                    next_items.append((value, dict(trail)))
            current_items = next_items
        return [(item, trail) for item, trail in current_items if isinstance(item, dict)]

    @staticmethod
    def _get_dotted_value(source: Any, dotted_path: str) -> Any:
        current = source
        for part in dotted_path.split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current

    @classmethod
    def _get_property_value(
        cls,
        prop_path: str,
        row: Dict[str, Any],
        trail: Dict[str, Dict[str, Any]],
        inject_obj: Optional[Dict[str, Any]],
        inject_as: str,
    ) -> Any:
        # Explicit context prefixes (data.foo, measurement.bar, inject.baz)
        if "." in prop_path:
            first, rest = prop_path.split(".", 1)
            if first == inject_as and inject_obj is not None:
                return cls._get_dotted_value(inject_obj, rest)
            if first in trail:
                return cls._get_dotted_value(trail[first], rest)

        # Try direct lookup in current row (supports dotted/nested).
        value = cls._get_dotted_value(row, prop_path)
        if value is not None:
            return value

        # Fallback: search in trail objects by path.
        for trail_obj in trail.values():
            value = cls._get_dotted_value(trail_obj, prop_path)
            if value is not None:
                return value

        # Final fallback for plain keys.
        if "." not in prop_path:
            if prop_path in row:
                return row.get(prop_path)
            for trail_obj in trail.values():
                if prop_path in trail_obj:
                    return trail_obj.get(prop_path)
        return None

    def convert_plot_togeojson(
        self,
        output_json,
        *,
        rows_key: str = "rows",
        records_path: Optional[Sequence[str]] = None,
        geometry_field: str = "polygon",
        coordinates_field: str = "coordinates",
        geometry_type: Optional[str] = None,
        geometry_type_field: str = "type",
        id_field: str = "id",
        output_id_property: str = "plotID",
        include_properties: Optional[List[str]] = None,
        include_all_properties: bool = False,
        skip_empty_coordinates: bool = True,
        include_path_ids: bool = False,
        path_id_field: str = "id",
        path_id_suffix: str = "_id",
        path_id_property_map: Optional[Dict[str, str]] = None,
        inject_input_json: Optional[str] = None,
        inject_records_path: Optional[Sequence[str]] = None,
        inject_id_field: str = "id",
        inject_match_field: Optional[str] = None,
        inject_as: str = "inject",
    ):
        source_data: Dict[str, Any]
        if self.input_dict is None:
            with open(self.input_json, "r", encoding="utf-8") as file:
                source_data = json.load(file)
        else:
            source_data = self.input_dict

        resolved_records_path = list(records_path) if records_path else [rows_key]
        rows_with_trail = self._resolve_records(source_data, resolved_records_path)

        inject_lookup: Dict[Any, Dict[str, Any]] = {}
        if inject_input_json:
            with open(inject_input_json, "r", encoding="utf-8") as inject_file:
                inject_data = json.load(inject_file)
            inject_path = list(inject_records_path) if inject_records_path else [rows_key]
            inject_rows_with_trail = self._resolve_records(inject_data, inject_path)
            for inject_row, _ in inject_rows_with_trail:
                inject_id = inject_row.get(inject_id_field)
                if inject_id is not None:
                    inject_lookup[inject_id] = inject_row

        include_properties = include_properties or [
            "area",
            "status",
            "plotName",
            "plotLabels",
            "plotNote",
            "plotVillage",
            "plotDistrict",
            "plotAdditionalData",
            "externalId",
        ]

        features = []
        for row, trail in rows_with_trail:
            geometry_data = row.get(geometry_field, {})
            coordinates = geometry_data.get(coordinates_field) if isinstance(geometry_data, dict) else None
            if skip_empty_coordinates and not coordinates:
                continue

            inject_obj: Optional[Dict[str, Any]] = None
            if inject_lookup and inject_match_field:
                match_value = self._get_property_value(
                    inject_match_field,
                    row=row,
                    trail=trail,
                    inject_obj=None,
                    inject_as=inject_as,
                )
                inject_obj = inject_lookup.get(match_value)

            if include_all_properties:
                props = {k: v for k, v in row.items() if k != geometry_field}
            else:
                props = {}
                for key_spec in include_properties:
                    # Flexible spec syntax:
                    # 1) "a.b.c" -> output key "a.b.c", single lookup path
                    # 2) "project.name=activityTemplate.project.name||data.activity_filter.activityTemplate.project.name"
                    #    -> output key "project.name", first non-null from fallback paths
                    if "=" in key_spec:
                        output_key, expression = key_spec.split("=", 1)
                        output_key = output_key.strip()
                        candidate_paths = [p.strip() for p in expression.split("||") if p.strip()]
                    else:
                        output_key = key_spec.strip()
                        candidate_paths = [output_key]

                    value = None
                    for candidate_path in candidate_paths:
                        value = self._get_property_value(
                            candidate_path,
                            row=row,
                            trail=trail,
                            inject_obj=inject_obj,
                            inject_as=inject_as,
                        )
                        if value is not None:
                            break
                    props[output_key] = value
            props[output_id_property] = row.get(id_field)

            if include_path_ids:
                for path_key, path_obj in trail.items():
                    if not isinstance(path_obj, dict):
                        continue
                    if path_id_field not in path_obj:
                        continue
                    output_key = (path_id_property_map or {}).get(path_key, f"{path_key}{path_id_suffix}")
                    # Do not overwrite explicit current record ID property.
                    if output_key == output_id_property:
                        continue
                    props[output_key] = path_obj.get(path_id_field)

            owner = row.get("owner", {}) if isinstance(row.get("owner", {}), dict) else {}
            if owner:
                props.update(
                    {
                        "firstName_owner": owner.get("firstName"),
                        "lastName_owner": owner.get("lastName"),
                        "email_owner": owner.get("email"),
                        "phoneNumber_owner": owner.get("phoneNumber"),
                        "country_owner": owner.get("country"),
                        "username_owner": owner.get("username"),
                        "gdprAccepted_owner": owner.get("gdprAccepted"),
                        "status_owner": owner.get("status"),
                    }
                )

            resolved_geometry_type = (
                geometry_type
                or (geometry_data.get(geometry_type_field) if isinstance(geometry_data, dict) else None)
                or "GeometryCollection"
            )

            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": resolved_geometry_type, "coordinates": coordinates},
                    "properties": props,
                }
            )

        geojson = {"type": "FeatureCollection", "features": features}
        with open(output_json, "w", encoding="utf-8") as output_file:
            json.dump(geojson, output_file, ensure_ascii=False, indent=2)
        print(f"GeoJSON written to {output_json}")
        return geojson

    def gpd_geojson(self, gdf, file_output_location):
        try:
            gdf.to_file(file_output_location, driver="GeoJSON")
        except ValueError:
            print("Invalid field type encountered.")
            for column in gdf.columns:
                if gdf[column].dtype == "object" and gdf[column].apply(lambda x: isinstance(x, list)).any():
                    gdf[column] = gdf[column].astype(str)
            gdf.to_file(file_output_location, driver="GeoJSON")
        return file_output_location