import os
import subprocess


def load_vault_env(allowed_prefixes: tuple[str, ...] | None = None) -> None:
    cmd = (
        "set -a; "
        "source /data/aturov/vault/scripts/export-env.sh kv/data/dev/clickhouse; "
        "env"
    )

    try:
        result = subprocess.run(
            ["/bin/bash", "-lc", cmd],
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception:
        return

    for line in result.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if allowed_prefixes is None or key.startswith(allowed_prefixes):
            os.environ[key] = value

load_vault_env()