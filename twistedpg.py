#module twistedpg.py
#Author: Federico Di Gregorio

from psycopg2 import *
from psycopg2 import _psycopg as _2psycopg
from psycopg2.extensions import register_type, UNICODE, connection as _2connection
from psycopg2.extras import RealDictCursor

del connect
def connect(*args, **kwargs):
    kwargs['connection_factory'] = connection
    return _2psycopg.connect(*args, **kwargs)

class connection(_2connection):
    def cursor(self):
        return _2connection.cursor(self, cursor_factory=RealDictCursor)

#register_type(UNICODE)