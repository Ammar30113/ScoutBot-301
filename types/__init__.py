from __future__ import annotations

import importlib.util as _importlib_util
import os as _os
import sys as _sys

# Mirror the standard library `types` module to avoid shadowing issues.
_stdlib_types_path = _os.path.join(_os.path.dirname(_os.__file__), "types.py")
_spec = _importlib_util.spec_from_file_location("_stdlib_types", _stdlib_types_path)
if _spec and _spec.loader:
    _stdlib_types = _importlib_util.module_from_spec(_spec)
    _spec.loader.exec_module(_stdlib_types)  # type: ignore[misc]
    for _name in dir(_stdlib_types):
        if not _name.startswith("__"):
            globals()[_name] = getattr(_stdlib_types, _name)
    __all__ = getattr(_stdlib_types, "__all__", [])
else:  # pragma: no cover - defensive
    __all__ = []

del _importlib_util, _os, _sys
