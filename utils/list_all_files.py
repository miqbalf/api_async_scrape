from datetime import datetime
from pathlib import Path
from typing import List

today = datetime.today().strftime("%Y-%m-%d")


def _sanitize_name(value: str) -> str:
    return (
        str(value)
        .replace("/", "_")
        .replace(" ", "_")
        .replace(".", "_")
        .replace("-", "_")
    )


def list_files(directory) -> List[str]:
    path = Path(directory)
    if not path.exists():
        return []
    return sorted(str(p) for p in path.iterdir() if p.is_file())


def get_name_folder(file_location):
    base_folder = Path(file_location).parent
    print("Current folder name:", base_folder)
    return str(base_folder / f"downloaded{today}.json")


def create_folder_file(basefolder, folder_name, name_file):
    folder_name_final = Path(basefolder) / str(folder_name)
    folder_name_final.mkdir(parents=True, exist_ok=True)
    safe_name = _sanitize_name(name_file)
    return str(folder_name_final / f"plot_{today}_{safe_name}.json")


def check_modified(folder_path):
    folder = Path(folder_path)
    if not folder.exists():
        print(f"Folder not found: {folder_path}")
        return
    for file_path in sorted(p for p in folder.iterdir() if p.is_file()):
        modified_date = datetime.fromtimestamp(file_path.stat().st_mtime).date().isoformat()
        print(f"File: {file_path.name}, Modified Date: {modified_date}")