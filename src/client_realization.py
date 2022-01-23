from .base_client import BaseClient
from . import ConnectionConfig, DefaultConnectionConfig

def BuildIndicatorClient(connection_config:ConnectionConfig=DefaultConnectionConfig):
    return BaseClient("indicator",connection_config)

def BuildMetricClient(connection_config:ConnectionConfig=DefaultConnectionConfig):
    return BaseClient("metric",connection_config)

def BuildCorrelationClient(connection_config:ConnectionConfig=DefaultConnectionConfig):
    return BaseClient("correlation",connection_config)