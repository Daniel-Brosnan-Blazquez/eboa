"""
Functions definition for the analysis component

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Auxiliary functions
def adjust_column_width(ws):
    """
    """
    for column in ws.columns:
        length = max(len(str(cell.value)) for cell in column)
        ws.column_dimensions[column[0].column].width = length + 2
    # end for
    return
