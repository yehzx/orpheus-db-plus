class MySQLConnectionError(Exception):
    def __init__(self, message):
        message = f"Reason: {type(message).__name__} - {message}"
        self.message = message