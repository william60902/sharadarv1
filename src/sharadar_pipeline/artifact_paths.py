"""Portable, deployment-scoped paths without rewriting historical evidence."""

from pathlib import Path


def resolve_artifact_path(root: Path, declared: str, *, deployment: str) -> Path:
    if (
        deployment not in {"dev", "prod"}
        or not isinstance(declared, str)
        or not declared
    ):
        raise ValueError("invalid artifact path scope")
    root = root.resolve()
    path = Path(declared)
    if ".." in path.parts or declared.startswith("~"):
        raise ValueError("artifact path traversal is prohibited")
    if not path.is_absolute():
        candidate = root / path
    elif path.is_relative_to(root):
        candidate = path
    else:
        # Exact known mount roots, never a substring search for sharadar/prod.
        prefixes = (
            Path(f"/Volumes/Pentagon_Quant/Medina_US_Equity/sharadar/{deployment}"),
            Path(f"/Volumes/Medina_US_Equity/sharadar/{deployment}"),
            Path(f"/mnt/nas/Medina_US_Equity/sharadar/{deployment}"),
        )
        prefix = next(
            (prefix for prefix in prefixes if path.is_relative_to(prefix)), None
        )
        if prefix is None:
            raise ValueError("artifact path is outside approved roots")
        candidate = root / path.relative_to(prefix)
    resolved = candidate.resolve()
    if not resolved.is_relative_to(root):
        raise ValueError("artifact path escapes deployment root")
    return resolved
