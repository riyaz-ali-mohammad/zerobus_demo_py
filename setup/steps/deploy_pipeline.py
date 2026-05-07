"""Upload sensor_pipeline.py to /Workspace/Shared/zerobus_demo/ and create the pipeline."""

from __future__ import annotations

import json
from pathlib import Path

from databricks.sdk.service.pipelines import FileLibrary, PipelineLibrary
from databricks.sdk.service.workspace import ImportFormat, Language

from setup.client import workspace_client
from setup.config import REPO_ROOT, Config

PIPELINE_DIR = REPO_ROOT / "pipeline"
LOCAL_PIPELINE_FILE = PIPELINE_DIR / "sensor_pipeline.py"
PIPELINE_SPEC = PIPELINE_DIR / "pipeline.json"
REMOTE_DIR = "/Workspace/Shared/zerobus_demo"
REMOTE_FILE = f"{REMOTE_DIR}/sensor_pipeline.py"


def _find_pipeline_id(w, name: str) -> str | None:
    for p in w.pipelines.list_pipelines(filter=f"name LIKE '{name}'"):
        if p.name == name:
            return p.pipeline_id
    return None


def run(cfg: Config, start: bool = False) -> str:
    w = workspace_client(cfg.profile)

    print(f">> creating {REMOTE_DIR}")
    w.workspace.mkdirs(REMOTE_DIR)

    print(f">> uploading {LOCAL_PIPELINE_FILE.name}")
    with LOCAL_PIPELINE_FILE.open("rb") as f:
        w.workspace.upload(
            path=REMOTE_FILE,
            content=f,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )

    spec = json.loads(PIPELINE_SPEC.read_text())
    libraries = [
        PipelineLibrary(file=FileLibrary(path=lib["file"]["path"]))
        for lib in spec.get("libraries", [])
    ]

    existing_id = _find_pipeline_id(w, spec["name"])
    if existing_id is not None:
        print(f">> pipeline {spec['name']} already exists ({existing_id}); skipping create")
        pipeline_id = existing_id
    else:
        print(f">> creating pipeline {spec['name']}")
        resp = w.pipelines.create(
            name=spec["name"],
            edition=spec.get("edition"),
            continuous=spec.get("continuous"),
            serverless=spec.get("serverless"),
            catalog=spec.get("catalog"),
            target=spec.get("target"),
            libraries=libraries,
            configuration=spec.get("configuration"),
            channel=spec.get("channel"),
            development=spec.get("development"),
        )
        pipeline_id = resp.pipeline_id
        print(f">> created pipeline_id={pipeline_id}")

    if start:
        print(f">> starting update for {pipeline_id}")
        w.pipelines.start_update(pipeline_id=pipeline_id)
    else:
        print(">> done — start the pipeline from the Lakeflow UI, or run:")
        print(f"     python -m setup.cli deploy-pipeline --start")

    return pipeline_id
