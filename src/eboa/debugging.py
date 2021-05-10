"""
Debugging definition

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import logging
from functools import wraps
import datetime

# Import logging
from eboa.logging import Log

logging_module = Log(name = __name__)
logger = logging_module.logger

def debug(method):
    """
    Function for profiling methods when logging level is DEBUG
    
    :param method: method to be profiled
    :type method: function

    :return: returned value of the method
    :rtype: int
    """
    @wraps(method)
    def _wrapper(*args, **kwargs):
        logger.debug("Method {} is going to be executed.".format(method.__name__))
        if logger.level == logging.DEBUG:
            start = datetime.datetime.now()
        # end if
        exit_value = method(*args, **kwargs)
        if logger.level == logging.DEBUG:
            stop = datetime.datetime.now()
            logger.debug("Method {} lasted {} seconds.".format(method.__name__, (stop - start).total_seconds()))
        # end if
        return exit_value

    return _wrapper

def race_condition():
    """ Commit function for race conditions checks """
    return

def race_condition2():
    """ Commit function for race conditions checks """
    return

def race_condition3():
    """ Commit function for race conditions checks """
    return
