#!/usr/bin/env python

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))

from common_precommit_utils import (
    console,
    initialize_breeze_precommit,
    run_command_via_breeze_shell,
)

initialize_breeze_precommit(__name__, __file__)

ALLOWED_FOLDERS = [
    "airflow",
]

if len(sys.argv) < 2:
    console.print(
        f"[yellow]You need to specify the folder"
        f" to test as parameter: {ALLOWED_FOLDERS}\n"
    )
    sys.exit(1)

mypy_folder = sys.argv[1]
if mypy_folder not in ALLOWED_FOLDERS:
    console.print(
        f"[yellow]Wrong folder {mypy_folder}."
        f" It should be one of those: {ALLOWED_FOLDERS}\n"
    )
    sys.exit(1)

arguments = [mypy_folder]
if mypy_folder == "providers/src/airflow/providers":
    arguments.extend(
        [
            "providers/tests",
            "--namespace-packages",
        ]
    )
if mypy_folder == "task_sdk/src/airflow/sdk":
    arguments.extend(
        [
            "task_sdk/tests",
            "--namespace-packages",
        ]
    )

if mypy_folder == "airflow":
    arguments.extend(
        [
            "tests",
        ]
    )

print(
    "Running /opt/airflow/scripts/in_container/run_mypy.sh with arguments: ", arguments
)

res = run_command_via_breeze_shell(
    [
        "/opt/airflow/scripts/in_container/run_mypy.sh",
        *arguments,
    ],
    warn_image_upgrade_needed=True,
    extra_env={
        "INCLUDE_MYPY_VOLUME": os.environ.get("INCLUDE_MYPY_VOLUME", "true"),
        # Need to mount local sources when running it - to not have to rebuild the image
        # and to let CI work on it when running on PRs from forks - because mypy-dev uses files
        # that are not available at the time when image is built in CI
        "MOUNT_SOURCES": "selected",
    },
)
ci_environment = os.environ.get("CI")
if res.returncode != 0:
    if ci_environment:
        console.print(
            "[yellow]You are running mypy with the folders selected. If you want to "
            "reproduce it locally, you need to run the following command:\n"
        )
        console.print("pre-commit run --hook-stage manual mypy-<folder> --all-files\n")
    upgrading = os.environ.get("UPGRADE_TO_NEWER_DEPENDENCIES", "false") != "false"
    if upgrading:
        console.print(
            "[yellow]You are running mypy with the image that has dependencies upgraded automatically.\n"
        )
    FLAG = " --upgrade-to-newer-dependencies" if upgrading else ""
    console.print(
        "[yellow]If you see strange stacktraces above, and can't reproduce it, please run"
        " this command and try again:\n"
    )
    console.print(f"breeze ci-image build --python 3.9{FLAG}\n")
    console.print(
        "[yellow]You can also run `breeze down"
        " --cleanup-mypy-cache` to clean up the cache used.\n"
    )
sys.exit(res.returncode)
