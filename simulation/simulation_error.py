from werkzeug.exceptions import HTTPException


class SimulationError(HTTPException):
    code = 500
    description = "Simulation failed!"
