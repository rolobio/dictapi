import psycopg2
from dictorm import DictDB

def error(msg):
    return {'error':True, 'message':str(msg)}

OK = 200
CREATED = 201
BAD_REQUEST = 400
NOT_FOUND = 404


class APITable(object):

    def __init__(self, api, table):
        self.api = api
        self.table = table


    def GET(self, *a, **kw):
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
            # Keep moving down the object until the last reference is gotten
            while a:
                try:
                    referenced = referenced[a.pop(0)]
                except KeyError:
                    # Bad reference was passed
                    self.api.db_conn.rollback()
                    return (NOT_FOUND, error('No reference found'))
            self.api.db_conn.rollback()
            return (OK, referenced)
        if kw:
            try:
                entry = self.table.get_one(**kw)
            except psycopg2.DataError:
                self.api.db_conn.rollback()
                return (BAD_REQUEST, error('Invalid primary keys'))
        if not entry:
            self.api.db_conn.rollback()
            return (NOT_FOUND, error('No entry matching: {}'.format(str(kw))))
        self.api.db_conn.rollback()
        return (OK, entry)


    def PUT(self, **kw):
        wheres = {pk:kw[pk] for pk in self.table.pks if pk in kw}
        entry = None
        code = CREATED

        get_code, entry = self.GET(**wheres)
        if 200 <= get_code <= 300:
            # Entry already exists, update it
            entry.update(kw)
            entry.flush()
            code = OK
        elif get_code == 404:
            # No entry found, create it
            entry = self.table(**kw).flush()
        else:
            # Error occured
            return (get_code, entry)

        self.api.db_conn.commit()
        return (code, entry)


    def DELETE(self, *a, **kw):
        if len(a) > len(self.table.pks):
            return (BAD_REQUEST, error('Invalid primary keys'))

        get_code, entry = self.GET(*a, **kw)
        if get_code == 200:
            # Entry exists, delete it
            result = entry.delete()
            self.api.db_conn.commit()
            return (OK, result)
        else:
            # Error occured
            return (get_code, entry)



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



