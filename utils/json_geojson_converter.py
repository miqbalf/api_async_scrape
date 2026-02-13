import json
from typing import Any, Dict, List, Optional

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

    def convert_plot_togeojson(
        self,
        output_json,
        *,
        rows_key: str = "rows",
        geometry_field: str = "polygon",
        coordinates_field: str = "coordinates",
        geometry_type: str = "Polygon",
        id_field: str = "id",
        include_properties: Optional[List[str]] = None,
    ):
        rows = self.input_json_convert(rows_key=rows_key) if self.input_dict is None else list(self.input_dict.get(rows_key, []))
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
        for row in rows:
            polygon_data = row.get(geometry_field, {})
            coordinates = polygon_data.get(coordinates_field) if isinstance(polygon_data, dict) else None
            if not coordinates:
                continue

            props = {"plotID": row.get(id_field)}
            for key in include_properties:
                props[key] = row.get(key)

            owner = row.get("owner", {}) if isinstance(row.get("owner", {}), dict) else {}
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

            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": geometry_type, "coordinates": coordinates},
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