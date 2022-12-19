class ConnectionConfig:
    def __init__(self, host:str, port:int, username:str, password: str, authsource:str, database: str) -> None:
        """
        HOST = "192.168.0.1"
        PORT = 8080
        USERNAME = "root"
        PASSWORD = "root"
        AUTHSOURCE = "nntrade"
        DATABASE = "nntrade"
        """
        self.__host = host
        self.__port = port
        self.__username = username
        self.__password = password
        self.__authsource = authsource
        self.__database = database
        pass
    
    @property
    def host(self)->str:
        return self.__host
    
    @property
    def port(self)->str:
        return self.__port

    @property
    def username(self)->str:
        return self.__username

    @property
    def password(self)->str:
        return self.__password
    
    @property
    def authsource(self)->str:
        return self.__authsource

    @property 
    def database(self)->str:
        return self.__database
