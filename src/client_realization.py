from .base_client import BaseClient

def BuildIndicatorClient():
    _ret = BaseClient()
    _ret._collection_name = "indicator"
    return _ret

def BuildMetricClient():
    _ret = BaseClient()
    _ret._collection_name = "metric"
    return _ret

def BuildCorrelationClient():
    _ret = BaseClient()
    _ret._collection_name = "correlation"
    return _ret