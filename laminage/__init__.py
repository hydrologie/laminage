from .hec.alternatives import CreationAlternative
from .hec.base import BaseManager
# A hardcoded __all__ variable is necessary to appease
# `mypy --strict` running in projects that import xarray.
__all__ = (
    # Top-level functions
    "CreationAlternative",
    "BaseManager"
)