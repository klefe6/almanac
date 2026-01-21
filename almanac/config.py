"""
Centralized configuration for Almanac Futures.

Loads settings from environment variables with safe defaults and validation.
"""

import os
from dataclasses import dataclass
from typing import Dict, Any


def _parse_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _parse_int(value: str, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


@dataclass
class AppConfig:
    host: str
    port: int
    debug: bool
    cache_type: str
    cache_dir: str
    cache_default_timeout: int
    cache_threshold: int

    def to_cache_config(self) -> Dict[str, Any]:
        return {
            'CACHE_TYPE': self.cache_type,
            'CACHE_DIR': self.cache_dir,
            'CACHE_DEFAULT_TIMEOUT': self.cache_default_timeout,
            'CACHE_THRESHOLD': self.cache_threshold,
        }


def _default_cache_dir() -> str:
    base_dir = os.path.dirname(__file__)
    # Place cache alongside the package, in ..\.cache
    return os.path.abspath(os.path.join(base_dir, '..', '.cache'))


def _validate(cfg: AppConfig) -> AppConfig:
    # Port range
    if not (1 <= cfg.port <= 65535):
        cfg.port = 8086

    # Cache type
    if cfg.cache_type not in {"filesystem", "SimpleCache", "FileSystemCache"}:
        cfg.cache_type = "filesystem"

    # Ensure cache directory exists for filesystem cache
    if cfg.cache_type.lower() == "filesystem":
        try:
            os.makedirs(cfg.cache_dir, exist_ok=True)
        except Exception:
            cfg.cache_dir = _default_cache_dir()
            os.makedirs(cfg.cache_dir, exist_ok=True)

    # Timeouts and thresholds should be positive
    if cfg.cache_default_timeout <= 0:
        cfg.cache_default_timeout = 3600
    if cfg.cache_threshold <= 0:
        cfg.cache_threshold = 500

    return cfg


def get_config() -> AppConfig:
    host = os.getenv('ALMANAC_HOST', '127.0.0.1')
    port = _parse_int(os.getenv('ALMANAC_PORT', ''), 8086)
    debug = _parse_bool(os.getenv('ALMANAC_DEBUG', ''), True)

    cache_type = os.getenv('ALMANAC_CACHE_TYPE', 'filesystem')
    cache_dir = os.getenv('ALMANAC_CACHE_DIR', _default_cache_dir())
    cache_default_timeout = _parse_int(os.getenv('ALMANAC_CACHE_DEFAULT_TIMEOUT', ''), 3600)
    cache_threshold = _parse_int(os.getenv('ALMANAC_CACHE_THRESHOLD', ''), 500)

    cfg = AppConfig(
        host=host,
        port=port,
        debug=debug,
        cache_type=cache_type,
        cache_dir=cache_dir,
        cache_default_timeout=cache_default_timeout,
        cache_threshold=cache_threshold,
    )

    return _validate(cfg)


