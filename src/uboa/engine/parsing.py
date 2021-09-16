"""
Parsing definition for the engine module

Written by DEIMOS Space S.L. (dibb)

module uboa
"""
# Import exceptions
from uboa.engine.errors import ErrorParsingDictionary

def validate_data_dictionary(data):
    """
    """
    
    # Check that the operations key exists
    if len(data.keys()) != 1 or not "operations" in data.keys():
        raise ErrorParsingDictionary("operations key does not exist or there are other defined keys in the dictionary")
    # end if

    # Check that the operations key contains a list
    if type(data["operations"]) != list:
        raise ErrorParsingDictionary("operations key does not contain a list")
    # end if

    for item in data["operations"]:
        # Check that the item is dictionary
        if type(item) != dict:
            raise ErrorParsingDictionary("The item inside the operations key is not corresponding to a dictionary")
        # end if
        
        # Check if the dict contains the mode
        if not "mode" in item.keys():
            raise ErrorParsingDictionary("The operation does not contain the mode")
        # end if

        # Check that the mode contains a valid value
        if not item["mode"] in ["insert", "erase_and_insert", "force_insert"]:
            raise ErrorParsingDictionary("The mode does not correspond to an allowed value")
        # end if

        if item["mode"] in ["insert", "erase_and_insert", "force_insert"]:
            _validate_insert_structure(item)
        # end if

    # end for
    return

def _validate_insert_structure(data):

    check_items = [item in ["mode", "users", "roles"] for item in data.keys()]
    if False in check_items:
        raise ErrorParsingDictionary("The allowed tags inside the insert operation structure are: mode, users and roles")
    # end if

    if "users" in data:
        _validate_users(data["users"])
    # end if

    if "roles" in data:
        _validate_roles(data["roles"])
    # end if

    return

def _validate_users(data):

    if type(data) != list:
        raise ErrorParsingDictionary("The tag users has to be of type list")
    # end if

    for user in data:
        if type(user) != dict:
            raise ErrorParsingDictionary("The user inside the users structure has to be of type dict")
        # end if

        check_items = [item in ["email", "username", "group", "password", "active", "roles", "configurations"] for item in user.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside users structure are: email, username, group, password, active, roles and configurations")
        # end if

        # Mandatory tags
        if not "email" in user:
            raise ErrorParsingDictionary("The tag email is mandatory inside user structure")
        # end if
        if not type(user["email"]) == str:
            raise ErrorParsingDictionary("The tag email inside users structure has to be of type string")
        # end if
        if not "username" in user:
            raise ErrorParsingDictionary("The tag username is mandatory inside user structure")
        # end if
        if not type(user["username"]) == str:
            raise ErrorParsingDictionary("The tag username inside users structure has to be of type string")
        # end if
        if not "password" in user:
            raise ErrorParsingDictionary("The tag password is mandatory inside user structure")
        # end if
        if not type(user["password"]) == str:
            raise ErrorParsingDictionary("The tag password inside users structure has to be of type string")
        # end if

        # Optional tags
        if "active" in user and type(user["active"]) != str:
            raise ErrorParsingDictionary("The tag active inside the user structure has to be of type str. Received type: {}".format(type(user["active"])))
        # end if
        if "active" in user and not user["active"].lower() in ["false", "true"]:
            raise ErrorParsingDictionary("The tag active inside the user structure has to have one of the following values: false or true. Received value: {}".format(user["active"]))
        # end if
        if "roles" in user and type(user["roles"]) != list:
            raise ErrorParsingDictionary("The tag roles inside the user structure has to be of type list. Received type: {}".format(type(user["roles"])))
        elif "roles" in user:
            check_roles = [type(role) == str for role in user["roles"]]
            if False in check_roles:
                raise ErrorParsingDictionary("The values inside roles structure has to be of type str. Received values {}".format(user["roles"]))
            # end if
        # end if
        if "configurations" in user and type(user["configurations"]) != list:
            raise ErrorParsingDictionary("The tag configurations inside the user structure has to be of type list. Received type: {}".format(type(user["configurations"])))
        elif "configurations" in user:
            check_configurations = [type(configuration) == str for configuration in user["configurations"]]
            if False in check_configurations:
                raise ErrorParsingDictionary("The values inside configurations structure has to be of type str. Received values {}".format(user["configurations"]))
            # end if
        # end if

    # end for

    return

def _validate_roles(data):

    if type(data) != list:
        raise ErrorParsingDictionary("The tag roles has to be of type list")
    # end if

    for role in data:
        if type(role) != dict:
            raise ErrorParsingDictionary("The role inside the roles structure has to be of type dict")
        # end if

        check_items = [item in ["name", "description"] for item in role.keys()]
        if False in check_items:
            raise ErrorParsingDictionary("The allowed tags inside roles structure are: name and description")
        # end if

        # Mandatory tags
        if not "name" in role:
            raise ErrorParsingDictionary("The tag name is mandatory inside role structure")
        # end if
        if not type(role["name"]) == str:
            raise ErrorParsingDictionary("The tag name inside roles structure has to be of type string")
        # end if

        # Optional tags
        if "description" in role and type(role["description"]) != str:
            raise ErrorParsingDictionary("The tag description inside the role structure has to be of type str. Received type: {}".format(type(role["description"])))
        # end if

    # end for

    return

