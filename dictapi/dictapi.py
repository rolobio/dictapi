from dictorm import DictDB
from functools import partial, wraps
import psycopg2

def error(msg):
    return {'error':True, 'message':str(msg)}

OK = 200
CREATED = 201
BAD_REQUEST = 400
NOT_FOUND = 404

COLLECTION_SIZE = 20


def NoRead(column_name, call):
    @wraps(call)
    def func(*a, **kw):
        result = call(*a, **kw)
        # Remove the column without reporting it
        result[1].pop(column_name, None)
        return result
    return func


def NoWrite(column_name, call):
    @wraps(call)
    def func(*a, **kw):
        if column_name in kw:
            return (BAD_REQUEST,
                    error('Cannot write to {}'.format(column_name)))
        # Column Name not passed, execute the call
        return call(*a, **kw)
    return func


class HTTPMethod:

    def __init__(self, apitable, name):
        self.api = apitable.api
        self.apitable = apitable
        self.name = name
        self.table = apitable.table


    def restrict(self, column_name, modifier):
        self.call = modifier(column_name, self.call)


    def __call__(self, *a, **kw):
        return self.call(*a, **kw)



class GET(HTTPMethod):

    def call(self, *a, **kw):
        entry = None
        page = kw.pop('page', 1)
        if not kw and not a:
            # Collection has been requested
            offset = (page-1) * COLLECTION_SIZE
            entries = list(self.table.get_where().limit(COLLECTION_SIZE
                ).offset(offset))
            if not entries:
                self.api.db_conn.rollback()
                return (NOT_FOUND, error('No entries found in collection'))

            self.api.db_conn.rollback()
            return (OK, entries)
        elif not kw and len(a) == len(self.table.pks):
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
                return (BAD_REQUEST, error('Invalid primary keys'))
            except psycopg2.ProgrammingError:
                self.api.db_conn.rollback()
                return (BAD_REQUEST, error('Invalid names'))
        if not entry:
            self.api.db_conn.rollback()
            return (NOT_FOUND, error('No entry matching: {}'.format(str(kw))))
        self.api.db_conn.rollback()
        return (OK, entry)



class HEAD(HTTPMethod):

    def call(self, *a, **kw):
        return (self.apitable.GET(*a, **kw)[0], None)



class PUT(HTTPMethod):

    def __getattr__(self, name):
        attr = super().__getattr__(name)
        print(attr)
        return attr

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

        self.DELETE = DELETE(self, 'DELETE')
        self.GET = GET(self, 'GET')
        self.HEAD = HEAD(self, 'HEAD')
        self.PUT = PUT(self, 'PUT')



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



