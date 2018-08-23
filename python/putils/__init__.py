#!/usr/bin/python3
__version__ = '0.1.0'

try:
  import pkg_resources
  pkg_resources.declare_namespace(__name__)
except ImportError:
  import pkgutil
  __path__ = pkgutil.extend_path(__path__, __name__)

__all__ = [
    # modules
    '__version__',
    'FastBigQueryRetrieval',
    'TimeDimension',
    'AdvancedSQLQueries'
]