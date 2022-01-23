from .base_client import BaseClient

def BuildIndicatorClient():
    return BaseClient("indicator")

def BuildMetricClient():
    return BaseClient("metric")

def BuildCorrelationClient():
    return BaseClient("correlation")