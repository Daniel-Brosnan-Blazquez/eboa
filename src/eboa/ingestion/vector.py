"""
Helper module for managing vectors in the EBOA component

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities

# Import numpy and quaternions
import numpy as np
from pyquaternion import Quaternion
import math

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

def quaternion_conjugate(quaternion):
    '''
    Function to define the conjugate of a quaternion given in the form of:
    scalar, vector

    :param quaternion: quaternion in the form [w, x, y, z]
    :type quaternion: list

    :return: conjugate of the quaternion
    :rtype: list
    '''
    w, x, y, z = quaternion
    
    return [float(w), -float(x), -float(y), -float(z)]

def normalize_quaternion (q):
    '''
    Function normalize a quaternion given in the form of:
    scalar, vector

    :param quaternion: quaternion in the form [w, x, y, z]
    :type quaternion: list

    :return: normalized quaternion
    :rtype: tuple
    '''
    magnitude = math.sqrt (sum (n * n for n in q))
    return tuple (n / magnitude for n in q)

def quaternion_vector_rotation(quaternion, vector):
    '''
    Function to obtain the vector result of a vector rotated by a quaternion in the form of:
    scalar, vector

    :param quaternion: quaternion in the form [w, x, y, z]
    :type quaternion: list
    :param vector: vector in the form [x, y, z]
    :type vector: list

    :return: vector rotated by the quaternion
    :rtype: array
    '''
    qualified_quaternion = Quaternion(np.array(quaternion))
    
    return qualified_quaternion.rotate(vector)

def get_quaternion_between_vectors(v1, v2):
    '''
    Function to obtain the quaternion to rotate vector v1 to vector v2

    :param v1: vector in the form [x, y, z]
    :type v1: list
    :param v2: vector in the form [x, y, z]
    :type v2: list

    :return: quaternion to rotate vector v1 to vector v2
    :rtype: array
    '''

    v1 = v1 / np.linalg.norm(v1)
    v2 = v2 / np.linalg.norm(v2)
    c = np.dot(v1, v2)
    axis = np.cross(v1, v2)
    s = np.sqrt((1+c)*2)
    q = np.array([s/2.0, axis[0]/s, axis[1]/s, axis[2]/s])
    return q

def calculate_point_from_position(position, vector, distance = 1000000):
    '''
    Function to obtain a final position given an initial position a vector to point to the final position and the distance between the initial and final positions.
    This function is following this reference:
    https://stackoverflow.com/questions/55946607/to-locate-a-3d-point-from-an-existing-3d-point-using-a-vector-and-distance

    :param position: position in the form [x, y, z] (in meters)
    :type position: list
    :param vector: vector in the form [x, y, z]
    :type vector: list
    :param distance: distance between initial and final positions (in meters)
    :type distance: int

    :return: final position
    :rtype: array
    '''

    initial_position = np.array(position)
    unit_vector = vector / np.linalg.norm(vector)
    final_position = position + distance * unit_vector
    
    return list(final_position)

def calculate_vector_between_positions(position1, position2):
    '''
    Function to obtain the vector between positions

    :param position1: position in the form [x, y, z]
    :type position1: list
    :param position2: position in the form [x, y, z]
    :type position2: list

    :return: vector
    :rtype: list
    '''

    vector = []
    for dimension, coordinate1 in enumerate(position1):
      vector.append(position2[dimension]-coordinate1)
    # end if
    
    return vector

def calculate_vector_between_positions(position1, position2):
    '''
    Function to obtain the vector between positions

    :param position1: position in the form [x, y, z]
    :type position1: list
    :param position2: position in the form [x, y, z]
    :type position2: list

    :return: vector
    :rtype: list
    '''

    vector = []
    for dimension, coordinate1 in enumerate(position1):
      vector.append(position2[dimension]-coordinate1)
    # end if
    
    return vector

def unit_vector(vector):
    '''
    Returns the unit vector of the vector.
    '''
    
    return vector / np.linalg.norm(vector)

def angle_between_vectors(v1, v2):
    '''
    Returns the angle in radians between vectors 'v1' and 'v2'::
    '''
    
    v1_u = unit_vector(v1)
    v2_u = unit_vector(v2)
    return np.arccos(np.clip(np.dot(v1_u, v2_u), -1.0, 1.0))
