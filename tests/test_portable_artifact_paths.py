from pathlib import Path
import pytest
from sharadar_pipeline.artifact_paths import resolve_artifact_path
from sharadar_pipeline.readiness_storage import (
    _safe_artifact_path,
    ReadinessConfigurationError,
)


@pytest.mark.parametrize("deployment", ["dev", "prod"])
def test_exact_legacy_mounts_and_relative_keys(tmp_path, deployment):
    for prefix in [
        "/Volumes/Pentagon_Quant/Medina_US_Equity",
        "/Volumes/Medina_US_Equity",
        "/mnt/nas/Medina_US_Equity",
    ]:
        assert (
            resolve_artifact_path(
                tmp_path, f"{prefix}/sharadar/{deployment}/raw/x", deployment=deployment
            )
            == tmp_path / "raw/x"
        )
    assert (
        resolve_artifact_path(tmp_path, "raw/x", deployment=deployment)
        == tmp_path / "raw/x"
    )
    assert (
        resolve_artifact_path(tmp_path, str(tmp_path / "raw/x"), deployment=deployment)
        == tmp_path / "raw/x"
    )


@pytest.mark.parametrize(
    "path",
    [
        "../x",
        "raw/../../x",
        "/evil/sharadar/dev/x",
        "/mnt/nas/Medina_US_Equity/sharadar/prod/x",
        "/Volumes/Medina_US_Equity/sharadar/dev/../prod/x",
        "~/x",
    ],
)
def test_dev_rejects_escape_and_wrong_deployment(tmp_path, path):
    with pytest.raises(ReadinessConfigurationError):
        _safe_artifact_path(tmp_path, path)


def test_symlink_escape_rejected_after_relocation(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    (root / "escape").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError):
        resolve_artifact_path(
            root, "/Volumes/Medina_US_Equity/sharadar/dev/escape/x", deployment="dev"
        )
