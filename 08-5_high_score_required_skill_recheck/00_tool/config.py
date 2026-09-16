"""08-5 execution mode configuration.

The canonical setting is ``STEP_08_5_EXECUTION_MODE``.  The old
``ENABLE_08_5_BATCH_ORCHESTRATION`` flag remains an input only; callers must
not implement their own precedence/default rules.
"""

import argparse
import os
import sys
from typing import Mapping, Optional


MODE_ENV = "STEP_08_5_EXECUTION_MODE"
LEGACY_FLAG_ENV = "ENABLE_08_5_BATCH_ORCHESTRATION"
VALID_MODES = {"legacy", "batch"}


class ExecutionModeError(ValueError):
    """The supplied new/compatibility settings do not form one safe mode."""


def _optional_value(environ: Mapping[str, str], name: str) -> Optional[str]:
    if name not in environ:
        return None
    value = environ[name]
    if not isinstance(value, str) or not value:
        raise ExecutionModeError(f"{name} must not be empty")
    return value


def resolve_execution_mode(environ: Optional[Mapping[str, str]] = None) -> str:
    """Resolve the canonical mode and fail closed on invalid/conflicting input."""
    values = os.environ if environ is None else environ
    mode = _optional_value(values, MODE_ENV)
    old_flag = _optional_value(values, LEGACY_FLAG_ENV)

    if mode is not None and mode not in VALID_MODES:
        raise ExecutionModeError(
            f"{MODE_ENV} must be legacy or batch: {mode!r}"
        )
    if old_flag is not None and old_flag not in {"0", "1"}:
        raise ExecutionModeError(
            f"{LEGACY_FLAG_ENV} must be 0 or 1: {old_flag!r}"
        )

    compatibility_mode = None
    if old_flag is not None:
        compatibility_mode = "batch" if old_flag == "1" else "legacy"
    if mode is not None and compatibility_mode is not None and mode != compatibility_mode:
        raise ExecutionModeError(
            f"08-5 execution mode settings conflict: "
            f"{MODE_ENV}={mode!r}, {LEGACY_FLAG_ENV}={old_flag!r}"
        )
    return mode or compatibility_mode or "batch"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--print-mode", action="store_true")
    args = parser.parse_args()
    try:
        mode = resolve_execution_mode()
    except ExecutionModeError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    if args.print_mode:
        print(mode)
    return 0


if __name__ == "__main__":
    sys.exit(main())
