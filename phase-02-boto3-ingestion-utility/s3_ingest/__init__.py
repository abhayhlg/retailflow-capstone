# This file turns the directory into an importable Python package
from .client import S3IngestClient

__all__ = ["S3IngestClient"]