from datetime import datetime, date
from dictapi.dictapi import APITable as OrigAPITable, API as OrigAPI
from dictapi.dictapi import DATETIME_FORMAT, HTTP_METHODS
from functools import wraps
import cherrypy
import json
import types


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type {} not serializable".format(type(obj))
            ) # pragma: no cover


def json_out(func):
    @wraps(func)
    def wrapper(*a, **kw):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        code, entry = func(*a, **kw)
        cherrypy.response.status = code
        if entry == None:
            out = {}
        else:
            out = json.dumps(entry, default=json_serial).encode()
        return out
    return wrapper


class APITable:

    exposed = True

    def __init__(self, api, table):
        self.api = api
        self.table = table
        self.apitable = OrigAPITable(api, table)

        for method_name in HTTP_METHODS:
            if method_name not in dir(self.apitable):
                continue
            original_method = getattr(self.apitable, method_name)
            setattr(self, method_name, json_out(original_method))


    def _options(self):
        return sorted([i for i in dir(self) if i in HTTP_METHODS])


    def OPTIONS(self):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        options = self._options()
        cherrypy.response.headers['Allow'] = ', '.join(options)
        return json.dumps(options).encode()



class API(OrigAPI):

    @classmethod
    def table_factory(cls): return APITable


    def generate_config(self):
        config = {}
        for table_name in self.dictdb:
            config['/'+str(table_name)] = {
                    'request.dispatch':cherrypy.dispatch.MethodDispatcher()
                    }
        return config



