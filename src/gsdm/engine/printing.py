"""
Engine printing definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
import json

# Import SQLAlchemy utilities
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.sqltypes import String, DateTime, NullType
from sqlalchemy.dialects import postgresql

# Class for helping printing json
# class NonTextEncoder(json.JSONEncoder):
#     """
#     """
#     def default(self,obj):
#         try:
#             encoded_obj = json.JSONEncoder.default(self, obj)
#         except:
#             encoded_obj = str(obj)

#         return encoded_obj

class StringLiteral(String):
    """
    Class to teach SQLAlchemy how to literalize various things.
    """
    def literal_processor(self, dialect):
        
        super_processor = super(StringLiteral, self).literal_processor(dialect)

        # Always return the casting to string
        return str

class LiteralDialect(DefaultDialect):
    """
    Class to implement the dialect needed for every column type
    """
    colspecs = {
        # prevent various encoding explosions
        String: StringLiteral,
        # teach SA about how to literalize a datetime
        DateTime: StringLiteral,
        # don't format py2 long integers to NULL
        NullType: StringLiteral,
        postgresql.UUID: StringLiteral
    }


def literal_query(statement):
    """
    Method to print raw SQL statements from the SQLAlchemy statements
    :param statement: SQLAlchemy statement
    :type statement: sqlalchemy.sql.annotation

    Method used for debugging purpuses (not open to external usage)
    """
    sql_statement = statement.compile(dialect=LiteralDialect(), compile_kwargs={'literal_binds': True}).string
    return sql_statement
