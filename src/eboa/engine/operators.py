# Import python utilities
import operator

comparison_operators = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne
}

arithmetic_operators = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne
}

text_operators = {
    "like": "like",
    "notlike": "notlike",
    "in": "in_",
    "notin": "notin_",
}

regex_operators = {
    "regex": ""
}
