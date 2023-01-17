"""
Helper module for managing satellite swaths in the EBOA component

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import numpy as np
import math
from dateutil import parser
import datetime

# Import astropy utilities
from astropy.coordinates import SkyCoord, ITRS
from astropy.time import Time

# Import eboa vector
import eboa.ingestion.vector as eboa_vector

# Import eboa orbit
import eboa.ingestion.orbit as eboa_orbit

# Import eboa utilities
from eboa.engine.errors import InputError

##########
# Configurations
##########
# Set the radius of the Earth in km
earth_radius = 6378.1370
max_points = 200

###########
# Functions for helping with the generation of footprints of a satellite's instrument
###########
def get_footprint(start, stop, alpha, tle_string = None, semimajor = None, satellite_orbit = None, roll=0, pitch=0, yaw=0, interval=30):
    '''
    Function to obtain the footprint of the instrument of the satellite during a period.
    The attitude of the satellite is specified by roll, pitch and yaw.

    :param start: start of the footprint in ISO 8601 format
    :type start: str
    :param stop: stop of the footprint in ISO 8601 format
    :type stop: str
    :param alpha: aperture angle of the instrument
    :type alpha: float
    :param tle_string: TLE of the satellite with the following format:
    SATELLITE-INDICATOR
    1 NNNNNC NNNNNAAA NNNNN.NNNNNNNN +.NNNNNNNN +NNNNN-N +NNNNN-N N NNNNN
    2 NNNNN NNN.NNNN NNN.NNNN NNNNNNN NNN.NNNN NNN.NNNN NN.NNNNNNNNNNNNNN
    :type tle_string: str
    :param semimajor: semimajor axis of the orbit of the satellite
    :type semimajor: float
    :param satellite_orbit: object defining the orbit of the satellite
    :type satellite_orbit: orbit object
    :param satellite_orbit: object defining the orbit of the satellite
    :type satellite_orbit: orbit object
    :param roll: roll angle of the attitude of the satellite
    :type roll: float
    :param pitch: pitch angle of the attitude of the satellite
    :type pitch: float
    :param yaw: yaw angle of the attitude of the satellite
    :type yaw: float

    :return: satellite track and footprint coordinates with the following format
    coordinates = {
        "satellite_track": "lon1 lat1, ..., lonN latN",
        "satellite_footprint": "lon1 lat1, ..., lonN latN",
    }
    :rtype: dict
    '''

    # Correct inputs
    corrected_start_datetime = parser.parse(start).replace(tzinfo=None)
    corrected_start = corrected_start_datetime.isoformat(timespec="microseconds")
    corrected_stop_datetime = parser.parse(stop).replace(tzinfo=None)
    corrected_stop = corrected_stop_datetime.isoformat(timespec="microseconds")

    duration = (corrected_stop_datetime - corrected_start_datetime).total_seconds()
    if (duration / interval) > max_points:
        interval = duration / max_points
    # end if
    
    # Check parameters are complete
    if tle_string == None and satellite_orbit == None:
        raise InputError("tle_string or satellite_orbit parameters should be defined")
    # end if
    if tle_string == None and semimajor == None:
        raise InputError("tle_string or semimajor parameters should be defined")
    elif tle_string != None:
        # Obtain semimajor from TLE
        semimajor = eboa_orbit.get_semimajor(tle_string)
    # end if

    if satellite_orbit == None:
        # Get the satellite orbit defined by the TLE
        satellite_orbit = eboa_orbit.get_orbit(tle_string)
    # end if

    # Obtain the satellite position during the period
    time = corrected_start
    j = 0
    inertial_satellite_positions = []
    epochs = []
    while time < corrected_stop:
        time = (parser.parse(corrected_start) + datetime.timedelta(seconds=j*interval)).replace(tzinfo=None).isoformat(timespec="microseconds")
        if time > corrected_stop:
            time = corrected_stop
        # end if
        # Get position of the satellite associated to time in the Earth inertial frame
        error, position, velocity = eboa_orbit.get_ephemeris(satellite_orbit, time)

        # Register position
        inertial_satellite_positions.append(position[0])
        inertial_satellite_positions.append(position[1])
        inertial_satellite_positions.append(position[2])
        epochs.append(time)
        
        j += 1
    # end while

    # Transform satellite positions to the Earth fixed frame
    satellite_positions = eboa_orbit.satellite_positions_to_fixed(inertial_satellite_positions, epochs)
    
    # Get swath
    swath = get_satellite_swath(satellite_positions, alpha, roll, pitch, yaw, semimajor)

    return swath

def get_satellite_swath(satellite_positions, alpha, roll, pitch, yaw, semimajor):
    '''
    Function to obtain the satellite footprint coordinates with the following format:
    lon1 lat1, ..., lonN latN

    :param satellite_positions: list of satellite positions in the Earth fixed frame [x1, y1, z1, ..., xn, yn, zn]
    :type satellite_positions: list
    :param alpha: aperture angle of the instrument
    :type alpha: float
    :param roll: roll angle of the attitude of the satellite
    :type roll: float
    :param pitch: pitch angle of the attitude of the satellite
    :type pitch: float
    :param yaw: yaw angle of the attitude of the satellite
    :type yaw: float
    :param semimajor: semimajor axis of the orbit of the satellite
    :type semimajor: float

    :return: satellite track and footprint coordinates with the following format
    coordinates = {
        "satellite_track": "lon1 lat1, ..., lonN latN",
        "satellite_footprint": "lon1 lat1, ..., lonN latN",
    }
    :rtype: dict
    '''

    ###
    # TO CHECK PARAMETERS
    ###

    coordinates = {
        "satellite_tracks": [],
        "satellite_footprints": [],
    }

    satellite_track = ""
    satellite_footprint = ""
    
    # Calculate angles corresponding to the effect of the pitch
    if pitch != 0:
        pitch_radians = (pitch*2*math.pi)/360
        pitch_a_radians = math.asin(((semimajor)*math.sin(pitch_radians))/earth_radius)
        pitch_a_degrees = 180-(pitch_a_radians*360)/(2*math.pi)
        pitch_b_degrees = 180-pitch_a_degrees-pitch
        pitch_b_radians = (pitch_b_degrees*2*math.pi)/360
    # end if

    # Calculate angles corresponding to the aperture of the instrument seen from ground (using roll + alpha)
    alpha_radians = (alpha*2*math.pi)/360
    roll_radians = (roll*2*math.pi)/360
    roll_a1_radians = math.asin(((semimajor)*math.sin(roll_radians-alpha_radians))/earth_radius)
    roll_a1_degrees = 180-(roll_a1_radians*360)/(2*math.pi)
    roll_a2_radians = math.asin(((semimajor)*math.sin(roll_radians))/earth_radius)
    roll_a2_degrees = 180-(roll_a2_radians*360)/(2*math.pi)
    roll_a3_radians = math.asin(((semimajor)*math.sin(roll_radians+alpha_radians))/earth_radius)
    roll_a3_degrees = 180-(roll_a3_radians*360)/(2*math.pi)
    roll_b1 = 180-roll_a1_degrees-roll+alpha
    roll_b2 = 180-roll_a2_degrees-roll
    roll_b3 = 180-roll_a3_degrees-roll-alpha
    
    i = 0
    satellite_coordinates = []
    right_coordinates = []
    left_coordinates = []
    while i < len(satellite_positions):
        # Get X, Y and Z position of the satellite
        satellite_x = satellite_positions[i]
        satellite_y = satellite_positions[i+1]
        satellite_z = satellite_positions[i+2]

        satellite_position = SkyCoord(x=satellite_x, y=satellite_y, z=satellite_z, frame='itrs', unit=("km", "km", "km"), representation_type="cartesian")
        latitude = satellite_position.earth_location.lat.value
        longitude = satellite_position.earth_location.lon.value
        satellite_projection = SkyCoord(lat=latitude, lon=longitude, distance=earth_radius, frame='itrs', unit=("deg", "deg", "km"), representation_type="spherical")

        satellite_projection_x = satellite_projection.earth_location.x.value
        satellite_projection_y = satellite_projection.earth_location.y.value
        satellite_projection_z = satellite_projection.earth_location.z.value

        satellite_coordinates.append("{} {}".format(satellite_projection.earth_location.lon.value, satellite_projection.earth_location.lat.value))

        i += 3

        # Set the positions in the array of the sibling satellite position
        x_position_sibling = i
        y_position_sibling = i+1
        z_position_sibling = i+2
        rotation_axis_sign = 1
        if i >= len(satellite_positions):
            x_position_sibling = i-6
            y_position_sibling = i-5
            z_position_sibling = i-4
            rotation_axis_sign = -1
        # end if

        # Get X, Y and Z position for the following set
        satellite_sibling_x = satellite_positions[x_position_sibling]
        satellite_sibling_y = satellite_positions[y_position_sibling]
        satellite_sibling_z = satellite_positions[z_position_sibling]

        # Get perpendicular vector to satellite positions
        axis_pitch = np.cross([satellite_x, satellite_y, satellite_z], [satellite_sibling_x, satellite_sibling_y, satellite_sibling_z])*rotation_axis_sign

        # Define rotations for pitch
        if pitch != 0:
            axis_pitch_unit = axis_pitch / np.linalg.norm(axis_pitch)
            rotation_pitch_b = eboa_vector.define_rotation_axis([axis_pitch_unit[0], axis_pitch_unit[1], axis_pitch_unit[2]], pitch_b_degrees)

            satellite_projection_pitch_b = rotation_pitch_b.apply([satellite_projection_x, satellite_projection_y, satellite_projection_z])

            axis_roll = np.cross([satellite_projection_pitch_b[0], satellite_projection_pitch_b[1], satellite_projection_pitch_b[2]], [axis_pitch[0], axis_pitch[1], axis_pitch[2]])
        else:
            satellite_projection_pitch_b = [satellite_projection_x, satellite_projection_y, satellite_projection_z]
            axis_roll = np.cross([satellite_x, satellite_y, satellite_z], [axis_pitch[0], axis_pitch[1], axis_pitch[2]])
        # end if

        # Define rotations for roll + alpha
        axis_roll_unit = axis_roll / np.linalg.norm(axis_roll)

        rotation_roll_alpha_b1 = eboa_vector.define_rotation_axis([axis_roll_unit[0], axis_roll_unit[1], axis_roll_unit[2]], roll_b1)
        rotation_roll_alpha_b3 = eboa_vector.define_rotation_axis([axis_roll_unit[0], axis_roll_unit[1], axis_roll_unit[2]], roll_b3)

        satellite_projection_roll_b1 = rotation_roll_alpha_b1.apply([satellite_projection_pitch_b[0], satellite_projection_pitch_b[1], satellite_projection_pitch_b[2]])
        satellite_projection_roll_b3 = rotation_roll_alpha_b3.apply([satellite_projection_pitch_b[0], satellite_projection_pitch_b[1], satellite_projection_pitch_b[2]])

        # Obtain latitude and longitudes of the footprint
        footprint_right_position = satellite_projection_roll_b1
        footprint_left_position = satellite_projection_roll_b3

        footprint_right_position = SkyCoord(x=satellite_projection_roll_b1[0], y=satellite_projection_roll_b1[1], z=satellite_projection_roll_b1[2], frame='itrs', unit=("km", "km", "km"), representation_type="cartesian")
        right_coordinates.append("{} {}".format(footprint_right_position.earth_location.lon.value, footprint_right_position.earth_location.lat.value))

        footprint_left_position = SkyCoord(x=satellite_projection_roll_b3[0], y=satellite_projection_roll_b3[1], z=satellite_projection_roll_b3[2], frame='itrs', unit=("km", "km", "km"), representation_type="cartesian")
        left_coordinates.append("{} {}".format(footprint_left_position.earth_location.lon.value, footprint_left_position.earth_location.lat.value))

        
    # end while

    # Build up satellite coordinates
    satellite_coordinates_to_reverse = satellite_coordinates.copy()
    satellite_coordinates_to_reverse.reverse()
    satellite_track = ",".join(satellite_coordinates) + "," + ",".join(satellite_coordinates_to_reverse)

    # Build up satellite footrpint coordinates
    # Reverse left coordinates to join with right coordinates
    left_coordinates.reverse()    
    satellite_footprint = ",".join(right_coordinates) + "," + ",".join(left_coordinates)

    # Correct footprints in case they are crossing the antimeridian
    coordinates["satellite_tracks"] = correct_antimeridian_issue_in_footprint(satellite_track)
    coordinates["satellite_footprints"] = correct_antimeridian_issue_in_footprint(satellite_footprint)

    return coordinates

def correct_antimeridian_issue_in_footprint(coordinates):
    """Function to generate a list of footprints (cut when affected by the antimeridian issue) from a given string of
    coordinates with the following format:
    'longitude latitude,longitude latitude,longitude latitude'
    
    Note that the algorithm expects the coordinates to be closed (last coordinate equals to first).
    
    Example of coordinates corrected from negative to positive longitude:
    Input = '-179.99592 -74.16433,162.86178 -64.68046,170.32559 -62.88090,-170.92199 -71.51724,-179.99592 -74.16433'
    Output = ['-179.99592 -74.16433,-180.0 -74.1620727655799,-180.0 -67.33640462603759,-170.92199 -71.51724,-179.99592 -74.16433,-179.99592 -74.16433', '180.0 -74.1620727655799,162.86178 -64.68046,170.32559 -62.8809,180.0 -67.33640462603759,180.0 -74.1620727655799']

    Example of coordinates corrected from positive to negative longitude:
    Input = '177.70168 70.09508,176.21806 71.69579,-176.68693 72.24555,-175.74527 70.61387,177.70168 70.09508'
    Output = ['177.70168 70.09508,176.21806 71.69579,180.0 71.988835300063,180.0 70.27703274456933,177.70168 70.09508,177.70168 70.09508', '-180.0 71.988835300063,-176.68693 72.24555,-175.74527 70.61387,-180.0 70.27703274456933,-180.0 71.988835300063']

    Example of coordinates not corrected
    Input = '46.85375 80.07100,31.46458 74.70960,17.52212 75.89819,25.94555 81.89870,46.85375 80.07100'
    Output = ['46.85375 80.071,31.46458 74.7096,17.52212 75.89819,25.94555 81.8987,46.85375 80.071,46.85375 80.071']

    :param coordinates: string of coordinates with the following format:
    'longitude latitude,longitude latitude,longitude latitude'
    :type coordinates: str

    :return: list of footprints
    :rtype: list of str
    """

    longitude_latitudes = coordinates.split(",")

    intersections_with_antimeridian=0
    init_longitudes_after_intersect_antimeridian=[]
    polygon = []
    polygons = [polygon]
    polygon_index = 0
    reverse = False
    for i, longitude_latitude  in enumerate(longitude_latitudes):
        if i == 0:
            j = len(longitude_latitudes) - 1
        else:
            j = i - 1
        # end if
        
        longitude, latitude = longitude_latitude.split(' ')
        pre_longitude, pre_latitude = longitude_latitudes[j].split(' ')

        longitude = float(longitude)
        latitude = float(latitude)

        pre_longitude = float(pre_longitude)
        pre_latitude = float(pre_latitude)
        # Check if the polygon crosses the antimeridian
        if (pre_longitude * longitude) <0 and (abs(longitude) + abs(pre_longitude)) > 270:
            if pre_longitude > 0:
                # Move the longitude coordinates to be around G meridian to calculate intersection 
                pre_longitude_g_meridian = pre_longitude - 180
                longitude_g_meridian = 180 + longitude
                latitude_med =  pre_latitude - pre_longitude_g_meridian * ((latitude - pre_latitude ) / (longitude_g_meridian - pre_longitude_g_meridian))
            else:
                pre_longitude_g_meridian = 180 + pre_longitude
                longitude_g_meridian = longitude - 180
                latitude_med =  pre_latitude - pre_longitude_g_meridian * ((latitude - pre_latitude ) / (longitude_g_meridian - pre_longitude_g_meridian))
            # end if
            
            new_longitude = 180.0
            if longitude > 0:
                new_longitude = -180.0
            # end if
            polygon.append((new_longitude, latitude_med))
                
            if len(init_longitudes_after_intersect_antimeridian) > 0 and new_longitude == init_longitudes_after_intersect_antimeridian[intersections_with_antimeridian-1]:
                reverse = True
            elif reverse and polygon_index == 0:
                reverse = False
                polygon_index = len(polygons) - 1
            # end if
            
            if reverse:
                polygon_index = polygon_index - 1
            else:
                polygon_index = polygon_index + 1
            # end if
            
            if reverse:
                polygon = polygons[polygon_index]
            else:
                polygon = []
                polygons.append(polygon)
            # end if
            polygon.append((-1 * new_longitude, latitude_med))
            init_longitudes_after_intersect_antimeridian.append(-1 * new_longitude)
            intersections_with_antimeridian = intersections_with_antimeridian + 1
        # end if            

        # Insert the current coordinate
        polygon.append((longitude, latitude))
    # end for

    # Postgis (for at least version 2.5.1) accepts a minimum number of 4 coordinates for a polygon. So, when the number of coordinates is 2 they are just duplicated
    for polygon in polygons:
        if len(polygon) == 2:
            polygon.append(polygon[1])
            polygon.append(polygon[0])
        else:
            polygon.append(polygon[0])
        # end if    
    # end for
    
    footprints = []
    for polygon in polygons:
        footprint = ""
        for i, coordinate in enumerate(polygon):
            if i == len(polygon) - 1:
                footprint = footprint + str(coordinate[0]) + " " + str(coordinate[1])
            else:
                footprint = footprint + str(coordinate[0]) + " " + str(coordinate[1]) + ","
            # end if
        # end for
        footprints.append(footprint)
    # end for

    return footprints
