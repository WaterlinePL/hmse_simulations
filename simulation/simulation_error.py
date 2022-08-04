class SimulationError(Exception):

    def __init__(self, message: str, logs: str):
        super().__init__(message)
        self.logs = logs
