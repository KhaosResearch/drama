import importlib
import os
import sys
from pathlib import Path

from drama.config import settings
from drama.logger import get_logger

logger = get_logger(__name__)


def load_from_file(filepath: str):
    """
    Load module from absolute file path.
    """
    sys_path = sys.path.copy()

    try:
        filepath = os.path.abspath(filepath)
        sys.path.insert(0, os.path.dirname(filepath))

        module_name = os.path.basename(filepath)
        module_name = os.path.splitext(module_name)[0]
        module = __import__(module_name, {})
    finally:
        sys.path = sys_path

    return module


def load_from_module(module: str):
    """
    Load module from inside current package.
    """
    try:
        imported_module = importlib.import_module(module)
    except ModuleNotFoundError:
        raise ImportError(f"Could not import module {module}: some import packages were not found")
    except ImportError:
        raise ImportError(f"Could not import module {module}")
    return imported_module


def get_process_func(module: str, fnc_name: str = "execute"):
    """
    Get `fnc_name` function from module.
    """
    if settings.API_DEBUG and Path(module).absolute().is_file():
        logger.debug("Loading module from file (relative import)")
        process_module = load_from_file(filepath=module)
    else:
        process_module = load_from_module(module=module)

    try:
        if hasattr(process_module, fnc_name):
            return process_module.execute
        else:
            raise ImportError(f"No execute() function found in {module}")
    except ImportError as e:
        raise e
