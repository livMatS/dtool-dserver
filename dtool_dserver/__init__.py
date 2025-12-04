"""dtool storage broker for accessing datasets through dserver."""

try:
    from importlib.metadata import version, PackageNotFoundError
except ModuleNotFoundError:
    from importlib_metadata import version, PackageNotFoundError

try:
    __version__ = version(__name__)
except PackageNotFoundError:
    # package is not installed
    __version__ = "0.0.0"

from .storagebroker import DServerStorageBroker

__all__ = ['DServerStorageBroker', '__version__']
