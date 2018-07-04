"""
Engine printing definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

import json

class NonTextEncoder(json.JSONEncoder):
    """
    """
    def default(self,obj):
        try:
            encoded_obj = json.JSONEncoder.default(self, obj)
        except:
            encoded_obj = str(obj)

        return encoded_obj
