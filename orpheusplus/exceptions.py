class MySQLConnectionError(Exception):
    def __init__(self, message):
        message = f"{type(message)} - {message}"
        self.message = message