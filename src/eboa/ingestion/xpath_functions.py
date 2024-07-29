"""
Helper module for using functions in XPATH

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

# Import debugging
from eboa.debugging import debug

# Import operators
from eboa.engine.operators import comparison_operators

###
# Date helpers
###
#@debug
def compare_strings(dummy, str1, str2, operator):
    """
    Method to compare to strings in a xpath evaluation
    :param dummy: parameter not used sent by lxml
    :type dummy: None
    :param str1: first string
    :type str1: list containing a str
    :param str2: second string
    :type str2: lxml.etree._ElementUnicodeResult
    :param operator: comparison operator
    :type operator: str

    :return: result of the comparison operation
    :rtype: boolean

    """    

    op = comparison_operators[operator]
    
    return op(str1[0], str(str2))
