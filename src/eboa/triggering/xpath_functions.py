"""
Helper module for the ingestions of files of Sentinel-2 using functions in XPATH

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import Python utilities
import re

###
# String helpers
###

def match(dummy, node, input_text):
    """
    Method to check if input_text matches with node
    :param dummy: parameter not used by lxml
    :type dummy: None
    :param node: node with text for matching
    :type node: Etree element or attribute
    :param input_text: string with text to match
    :type input_text: str

    :return: True if there is match, False if not
    :rtype: str

    """
    try:
        match_text = "^" + node[0].text + "$"
        match = re.match(match_text, input_text)
    except AttributeError:
        match_text = "^" + node[0] + "$"
        match = re.match(match_text, input_text)
    # end try    
    if match:
        return True
    # end if

    return False
