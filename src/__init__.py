from multiprocessing import connection


class ConnectionConfig:
    HOST = "192.168.0.1"
    PORT = 8080
    USERNAME = "root"
    PASSWORD = "root"
    AUTHSOURCE = "nntrade"
    DATABASE = "nntrade"

DefaultConnectionConfig = ConnectionConfig()