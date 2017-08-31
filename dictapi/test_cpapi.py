from dictapi.test_dictapi import BaseTest
from dictapi.cpapi import API as CPAPI
from functools import partial
import cherrypy
import os
import psycopg2
import requests
import unittest


class BaseCherryPy(BaseTest):

    def setUp(self):
        super().setUp()

        self.api = CPAPI(self.conn)

        cherrypy.config.update({
            'log.screen':False,
            'log.access_file':'',
            'log.error_file':''
            })
        self.app = cherrypy.tree.mount(self.api, '/api',
                config=self.api.generate_config())
        cherrypy.engine.start()
        cherrypy.config.update({'log.screen':True})


    def __request(self, method, path, data={}):
        func = getattr(requests, method)
        r = func('http://127.0.0.1:8080/api'+path, data=data)
        return r.json()


    def get(self, *a, **kw):    return self.__request('get', *a, **kw)
    def put(self, *a, **kw):    return self.__request('put', *a, **kw)
    def post(self, *a, **kw):   return self.__request('post', *a, **kw)
    def delete(self, *a, **kw): return self.__request('delete', *a, **kw)
    def head(self, *a, **kw):   return self.__request('head', *a, **kw)



class TestAPICherryPy(BaseCherryPy):


    def test_simple(self):
        """
        Test the simple functionality of a RESTFul API
        """
        # Insert Jake
        jake1 = self.put('/person', data={'name':'Jake'})
        print(jake1)
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


    def test_get_non_existant(self):
        """
        GETing an entry that doesn't exist raises an error
        """
        error = self.get('/person', data={'id':1})
        self.assertIn('error', error)

        # Incorrect primary keys also fails
        error = self.get('/person', data={'foo':1, 'bar':2})
        self.assertIn('error', error)


    def test_reference(self):
        """
        The person table references the department table using a join table.
        """
        sales = self.put('/department', data={'name':'Sales'})
        self.assertDictContains(sales, {'id':1, 'name':'Sales'})


