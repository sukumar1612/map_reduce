class InvalidSplitSize(Exception):
    def __init__(self, message="split size is not > 0"):
        self.message = message
        super().__init__(self.message)
