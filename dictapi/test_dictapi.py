from dictapi import API
from dictapi.cpapi import APITable
from functools import partial
import cherrypy
import os
import psycopg2
import requests
import unittest


if 'CI' in os.environ.keys():
    test_db_login = {
            'database':'dictorm',
            'user':'postgres',
            'password':'',
            'host':'localhost',
            'port':'5432',
            }
else:
    test_db_login = {
            'database':'dictorm',
            'user':'dictorm',
            'password':'dictorm',
            'host':'localhost',
            'port':'5432',
            }


DB_SCHEMA = '''
CREATE TABLE person (
    id SERIAL PRIMARY KEY,
    name TEXT,
    manager_id INTEGER REFERENCES person(id)
)
'''

class BaseTest(unittest.TestCase):

    def setUp(self):
        self.conn = psycopg2.connect(**test_db_login)
        curs = self.conn.cursor()
        for _ in range(2):
            try:
                curs.execute(DB_SCHEMA)
                break
            except:
                self.drop_schema()
        self.conn.commit()


    def tearDown(self):
        self.drop_schema()
        self.conn.commit()


    def drop_schema(self):
        self.conn.rollback()
        curs = self.conn.cursor()
        curs.execute('''DROP SCHEMA public CASCADE;
                CREATE SCHEMA public;
                GRANT ALL ON SCHEMA public TO postgres;
                GRANT ALL ON SCHEMA public TO public;''')


    def assertDictContains(cls, a, b):
        if not set(b.items()).issubset(set(a.items())):
            raise TypeError('Dict is missing items {}'.format(
                str(dict(set(b.items()).difference(set(a.items()))))))



class BaseCherryPy(BaseTest):

    def setUp(self):
        super().setUp()

        self.api = API(self.conn)
        self.api.table_factory = lambda: APITable
        self.api.init_tables()

        cherrypy.config.update({
            'log.screen':False,
            'log.access_file':'',
            'log.error_file':''
            })
        self.app = cherrypy.tree.mount(self.api, '/', config={
            '/person':{'request.dispatch': cherrypy.dispatch.MethodDispatcher()}
            })
        cherrypy.engine.start()
        cherrypy.config.update({'log.screen':True})



    def request(self, method, path, data={}):
        func = getattr(requests, method)
        r = func('http://127.0.0.1:8080'+path, data=data)
        return r.json()


    def get(self, *a, **kw):    return self.request('get', *a, **kw)
    def put(self, *a, **kw):    return self.request('put', *a, **kw)
    def post(self, *a, **kw):   return self.request('post', *a, **kw)
    def delete(self, *a, **kw): return self.request('delete', *a, **kw)
    def head(self, *a, **kw):   return self.request('head', *a, **kw)



class TestAPICherryPy(BaseCherryPy):


    def test_simple(self):
        """
        Test the simple functionality of a RESTFul API
        """
        # Insert Jake
        jake1 = self.put('/person', data={'name':'Jake'})
        self.assertDictContains(jake1, {'id':1, 'name':'Jake'})

        # Get the same Jake
        jake2 = self.get('/person/1')
        self.assertEqual(jake1, jake2)

        # Get the same Jake using his name
        jake3 = self.get('/person', data={'name':'Jake'})
        self.assertEqual(jake1, jake2, jake3)


    def test_put(self):
        """
        An entry should be overwritten when using PUT
        """
        jake = self.put('/person', data={'name':'Jake'})

        # Name change
        jake['name'] = 'Phil'
        phil = self.put('/person', data=jake)

        self.assertDictContains(phil, {'id':1, 'name':'Phil'})


    def test_put_not_existing(self):
        """
        An error is returned when PUTing over a non-existant entry
        """
        # Jake does not exist, error is raised
        error = self.put('/person', data={'id':1, 'name':'Jake'})
        self.assertIn('error', error)


    def test_get_non_existant(self):
        """
        GETing an entry that doesn't exist raises an error
        """
        error = self.get('/person', data={'id':1})
        self.assertIn('error', error)


