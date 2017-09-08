from datetime import datetime
from dictorm import DictDB
from functools import wraps
import psycopg2

__all__ = ['COLLECTION_SIZE', 'API', 'APITable',
        'NoWrite',
        'NoRead'
        'LastModified',
        ]

COLLECTION_SIZE = 20
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'


def error(msg):
    return {'error':True, 'message':str(msg)}

OK = 200
CREATED = 201
BAD_REQUEST = 400
NOT_FOUND = 404

HTTP_METHODS = (
        'CONNECT',
        'DELETE',
        'GET',
        'HEAD',
        'OPTIONS',
        'PATCH',
        'POST',
        'PUT',
        'TRACE',
        )

def NoRead(call, column_name, *a, **kw):
    code, result = call(*a, **kw)
    # Remove the column without reporting it
    result.pop(column_name, None)
    return (code, result)


def NoWrite(call, column_name, *a, **kw):
    if column_name in kw:
        return (BAD_REQUEST,
                error('Cannot write to {}'.format(column_name)))
    # Column Name not passed, execute the call
    return call(*a, **kw)


def LastModified(call, column_name, *a, **kw):
    kw[column_name] = datetime.now()
    return call(*a, **kw)


class HTTPMethod:

    def __init__(self, apitable):
        self.api = apitable.api
        self.apitable = apitable
        self.table = apitable.table


    def modify(self, modifier, *a, **kw):
        call = self.call
        @wraps(self.call)
        def wrapper(*fa, **fkw):
            result = modifier(call, *a, *fa, **fkw, **kw)
            self.api.db_conn.rollback()
            return result
        self.call = wrapper


    def __call__(self, *a, **kw):
        return self.call(*a, **kw)



class GET(HTTPMethod):

    def call(self, *a, **kw):
        entry = None
        if not kw and len(a) == len(self.table.pks):
            # Convert positional arguments to keyword arguments if there are the
            # same amount of primary keys.  It is assumed that the arguments are
            # in the same order as the primary keys
            kw = dict(zip(self.table.pks, a))
        elif not kw and len(a) > len(self.table.pks):
            a = list(a)
            # Requesting a reference/substratum, get the primary keys for this
            # table
            wheres = {pk:a.pop(0) for pk in self.table.pks}
            # The object that contains references
            referenced = self.table.get_one(**wheres)
            if not referenced:
                self.api.db_conn.rollback()
                return (NOT_FOUND,
                        error('No entry matching: {}'.format(str(wheres))))
            # Keep moving down the object until the last reference is gotten
            while a:
                current = a.pop(0)
                if current not in referenced:
                    self.api.db_conn.rollback()
                    return (BAD_REQUEST, error('No reference exists'))
                referenced = referenced[current]
            self.api.db_conn.rollback()
            return (OK, referenced)
        if kw:
            try:
                entry = self.table.get_one(**kw)
            except psycopg2.DataError:
                self.api.db_conn.rollback()
                return (BAD_REQUEST, error('Invalid primary key(s)'))
            except psycopg2.ProgrammingError:
                self.api.db_conn.rollback()
                return (BAD_REQUEST, error('Invalid name(s)'))
        if not entry:
            self.api.db_conn.rollback()
            return (NOT_FOUND, error('No entry matching: {}'.format(str(kw))))
        self.api.db_conn.rollback()
        return (OK, entry)



class GET_RANGE(HTTPMethod):

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.maximum_range = COLLECTION_SIZE


    def call(self, ranges, *a, **kw):
        offset, end = 0, COLLECTION_SIZE
        if ranges:
            if '-' not in ranges or ranges.count('-') != 1:
                return (BAD_REQUEST, error('Invalid range value'))
            elif ranges.startswith('-'):
                # Only end is specified
                end = int(ranges.lstrip('-'))
            elif ranges.endswith('-'):
                # Only offset is specified
                offset = int(ranges.rstrip('-'))
            else:
                offset, end = ranges.split('-')
                offset, end = int(offset), int(end)

        # Range is inclusive
        offset = offset - 1 if offset > 0 else offset

        if offset >= end:
            return (BAD_REQUEST, error('Invalid range value'))

        limit = end - offset
        entries = list(self.table.get_where().offset(offset).limit(limit))
        self.api.db_conn.rollback()
        if not entries:
            return (NOT_FOUND, error('No entries found in range'))
        return (OK, entries)



class HEAD(HTTPMethod):

    def call(self, *a, **kw):
        return (self.apitable.GET(*a, **kw)[0], None)



class PUT(HTTPMethod):

    def call(self, *a, **kw):
        # Inserting an entry is the default
        get_code, entry = 404, None
        # Get the entry that matches the primary keys, otherwise the GET will
        # attempt to find an entry that may not yet contain the values we're
        # PUTing
        wheres = {pk:kw[pk] for pk in self.table.pks if pk in kw}
        if wheres:
            # Getting an entry is possible, get it
            get_code, entry = self.apitable.GET(**wheres)
        if (entry == None or not isinstance(entry, list))\
                and get_code == 200:
            # Entry already exists, update it
            entry.update(kw)
            entry.flush()
            self.api.db_conn.commit()
            return (OK, entry)
        elif get_code == 404:
            # No entry found, create it
            entry = self.table(**kw).flush()
            self.api.db_conn.commit()
            return (CREATED, entry)
        else:
            # Error occured
            return (get_code, entry)



class DELETE(HTTPMethod):

    def call(self, *a, **kw):
        if len(a) > len(self.table.pks):
            return (BAD_REQUEST, error('Invalid primary keys'))

        get_code, entry = self.apitable.GET(*a, **kw)
        if get_code == 200:
            # Entry exists, delete it
            try:
                result = entry.delete()
            except psycopg2.IntegrityError:
                # Can't delete the entry
                self.api.db_conn.rollback()
                return (BAD_REQUEST, error('Cannot delete referenced entry'))
            self.api.db_conn.commit()
            return (OK, result)
        else:
            # Error occured
            return (get_code, entry)



class APITable(object):

    def __init__(self, api, table):
        self.api = api
        self.table = table

        self.DELETE = DELETE(self)
        self.GET = GET(self)
        self.GET_RANGE = GET_RANGE(self)
        self.HEAD = HEAD(self)
        self.PUT = PUT(self)



class API(object):

    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.dictdb = DictDB(db_conn)
        self.init_tables()


    def init_tables(self):
        for table_name in self.dictdb:
            table = self.dictdb[table_name]
            apitable = self.table_factory()
            setattr(self, table_name, apitable(self, table))


    @classmethod
    def table_factory(cls): return APITable



