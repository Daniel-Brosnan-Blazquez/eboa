"""
Logging definition

Written by DEIMOS Space S.L. (dibb)

module uboa
"""
# Import EBOA logging functions
from eboa.logging import Log

class Log(Log):

    def __init__(self, name = None, log_name = "uboa_engine.log"):
        self.log_name = log_name
        self.define_logging_configuration(name)

