"""
Helper module for managing vectors in the EBOA component

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import numpy as np

# Import scipy utilities
from scipy.spatial.transform import Rotation as R

def define_rotation_axis(axis, degrees):
    '''
    Function to define the rotation axis given the axis and the degrees to rotate

    :param axis: List of X, Y and Z values
    :type axis: list
    :param degrees: degrees to rotate
    :type degrees: float

    :return: rotation to apply
    :rtype: rotation
    '''

    rotation_vector = np.radians(degrees) * np.array(axis)

    return R.from_rotvec(rotation_vector)
