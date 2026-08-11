"""Delete turbopuffer namespaces left behind by a CI run.

The e2e fixtures delete their own namespaces, but a cancelled or killed job
never reaches teardown. CI tags every namespace it creates with
MZ_TPUF_E2E_PREFIX so this can clean up precisely, without touching anything
another run — or a person — is using.
"""

from __future__ import annotations

import os
import sys

from turbopuffer import Turbopuffer


def main() -> int:
    prefix = os.environ.get("MZ_TPUF_E2E_PREFIX")
    api_key = os.environ.get("MZ_TPUF_TURBOPUFFER_API_KEY")
    if not prefix or not api_key:
        print("no prefix or API key; nothing to clean up")
        return 0

    marker = f"mz-tpuf-e2e-{prefix}-"
    client = Turbopuffer(
        api_key=api_key,
        region=os.environ.get("MZ_TPUF_TURBOPUFFER_REGION", "aws-us-east-1"),
    )

    failures = 0
    for namespace in client.namespaces():
        if not namespace.id.startswith(marker):
            continue
        try:
            client.namespace(namespace.id).delete_all()
            print(f"deleted {namespace.id}")
        except Exception as exc:  # best effort; never fail the job over cleanup
            print(f"could not delete {namespace.id}: {exc}")
            failures += 1
    if failures:
        print(f"{failures} namespace(s) could not be deleted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
