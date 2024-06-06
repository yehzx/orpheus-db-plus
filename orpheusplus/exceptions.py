class MySQLError(Exception):
    def __init__(self, error_code, msg):
        self.error_code = error_code
        self.msg = msg


class NonEmptyOperation(Exception):
    def __init__(self, msg):
        self.msg = msg
