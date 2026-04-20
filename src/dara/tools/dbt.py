from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class DbtManifest:
    def __init__(self, settings):
        self._path = Path(settings.dbt_manifest_path)
        self._manifest: dict | None = None

    def _load(self) -> dict:
        if self._manifest is None:
            if not self._path.exists():
                raise FileNotFoundError(f"dbt manifest not found at {self._path}")
            self._manifest = json.loads(self._path.read_text())
        return self._manifest

    def health(self) -> bool:
        self._load()
        return True

    def get_node(self, node_name: str) -> dict | None:
        manifest = self._load()
        nodes = manifest.get("nodes", {})
        for key, node in nodes.items():
            if node.get("name") == node_name or key.endswith(f".{node_name}"):
                return node
        return None

    def get_model_for_task(self, task_id: str) -> dict | None:
        """Try to match an Airflow task_id to a dbt model or test."""
        manifest = self._load()

        # dbt tests follow pattern: test_<model>_<column>_<constraint>
        # or the task_id IS the model name
        candidate = task_id.removeprefix("test_")

        for key, node in manifest.get("nodes", {}).items():
            name = node.get("name", "")
            if name == task_id or name == candidate or candidate in name:
                return node
        return None

    def get_upstream_models(self, node_unique_id: str, depth: int = 3) -> list[dict]:
        manifest = self._load()
        nodes = manifest.get("nodes", {})
        sources = manifest.get("sources", {})

        visited = set()
        result = []

        def traverse(uid: str, current_depth: int):
            if current_depth == 0 or uid in visited:
                return
            visited.add(uid)
            node = nodes.get(uid) or sources.get(uid)
            if node:
                result.append(node)
            deps = manifest.get("parent_map", {}).get(uid, [])
            for dep in deps:
                traverse(dep, current_depth - 1)

        traverse(node_unique_id, depth)
        return result

    def get_downstream_models(self, node_unique_id: str, depth: int = 3) -> list[dict]:
        manifest = self._load()
        nodes = manifest.get("nodes", {})

        visited = set()
        result = []

        def traverse(uid: str, current_depth: int):
            if current_depth == 0 or uid in visited:
                return
            visited.add(uid)
            node = nodes.get(uid)
            if node:
                result.append(node)
            children = manifest.get("child_map", {}).get(uid, [])
            for child in children:
                traverse(child, current_depth - 1)

        traverse(node_unique_id, depth)
        return result

    def get_test_info(self, test_name: str) -> dict | None:
        manifest = self._load()
        for key, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") == "test" and node.get("name") == test_name:
                return node
        return None

    def list_failed_tests(self, run_results_path: str | None = None) -> list[dict]:
        """Parse dbt run_results.json to find failed tests."""
        if run_results_path is None:
            run_results_path = str(self._path.parent / "run_results.json")

        path = Path(run_results_path)
        if not path.exists():
            return []

        run_results = json.loads(path.read_text())
        return [
            r for r in run_results.get("results", [])
            if r.get("status") in ("fail", "error")
        ]
