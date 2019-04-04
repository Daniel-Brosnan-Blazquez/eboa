"""
Helper module for managing EBOA entities in the EBOA component

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

###########
# Functions for helping with geometries
###########
def correct_list_of_coordinates (list_of_coordinates):
    """
    Method to correct the format of a given list of coordinates
    :param list_of_coordinates: list with coordinates
    :type list_of_coordinates: list

    :return: list_of_coordinates
    :rtype: str

    """
    if type(list_of_coordinates) != list:
        raise
    # end if
    result_list_of_coordinates = []
    # Minimum accpeted number of coordinates is 2
    if len(list_of_coordinates) > 1:
        first_longitude = list_of_coordinates[0]
        first_latitude = list_of_coordinates[1]
        result_list_of_coordinates.append(first_longitude)
        result_list_of_coordinates.append(first_latitude)
        i = 1
        while i < len(list_of_coordinates)/2:
            longitude = list_of_coordinates[i*2]
            if longitude == first_longitude:
                break
            elif ((i*2) + 1) < len(list_of_coordinates):
                latitude = list_of_coordinates[(i*2) + 1]
                result_list_of_coordinates.append(longitude)
                result_list_of_coordinates.append(latitude)
            # end if
            i += 1
        # end while
        result_list_of_coordinates.append(first_longitude)
        result_list_of_coordinates.append(first_latitude)
    # end if
        
    return result_list_of_coordinates

def list_of_coordinates_to_str_geometry (list_of_coordinates):
    """
    Method to receive a string of coordinates and return the same list but with a correct format
    :param list_of_coordinates: list with coordinates
    :type list_of_coordinates: list

    :return: geometry
    :rtype: str

    """
    result_geometry = ""
    i = 0
    for coordinate in list_of_coordinates:
        if i == 0:
            result_geometry = coordinate
        else:
            result_geometry = result_geometry + " " + coordinate
        # end if
        i += 1
    # end for
        
    return result_geometry
