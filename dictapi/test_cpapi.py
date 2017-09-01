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


    def __request(self, method, path, data=None):
        request = requests.Request(method, 'http://127.0.0.1:8080/api'+path,
                data=data)
        request = request.prepare()
        response = requests.Session().send(request)
        return response


    def get(self, *a, **kw):    return self.__request('get', *a, **kw)
    def put(self, *a, **kw):    return self.__request('put', *a, **kw)
    def post(self, *a, **kw):   return self.__request('post', *a, **kw)
    def delete(self, *a, **kw): return self.__request('delete', *a, **kw)
    def head(self, *a, **kw):   return self.__request('head', *a, **kw)


    def assertResponse(self, expected_code, response, expected_entry):
        self.assertEqual(expected_code, response.status_code)
        entry = response.json()
        self.assertDictContains(entry, expected_entry)
        return entry




class TestAPICherryPy(BaseCherryPy):


    def test_get(self):
        """
        Test the simple functionality of a RESTFul API
        """
        # Insert Jake
        jake1 = self.api.person.PUT(name='Jake')

        # Get the same Jake
        response = self.get('/person/1')
        jake2 = self.assertResponse(200, response,
                {'id':1, 'name':'Jake'})


    def test_put(self):
        """
        An entry should be overwritten when using PUT
        """
        response = self.put('/person', data={'name':'Jake'})
        self.assertEqual(201, response.status_code)
        jake = response.json()

        # Name change
        jake['name'] = 'Phil'
        response = self.put('/person', data=jake)
        self.assertEqual(200, response.status_code)
        phil = response.json()

        self.assertDictContains(phil, {'id':1, 'name':'Phil'})


    def test_get_non_existant(self):
        """
        GETing an entry that doesn't exist raises an error
        """
        error = self.get('/person', data={'id':1})
        self.assertEqual(404, error.status_code)
        self.assertIn('error', error.json())

        # Incorrect primary keys also fails
        error = self.get('/person', data={'foo':1, 'bar':2})
        self.assertEqual(404, error.status_code)
        self.assertIn('error', error.json())


    def test_reference(self):
        """
        The person table references the department table using a join table.
        """
        # Setup the references for each table
        Person, Department = self.api.person.table, self.api.department.table
        PD = self.api.person_department.table
        Person['person_department'] = Person['id'] == PD['person_id']
        PD['department'] = PD['department_id'] == Department['id']
        # Use a substratum so the person_department entry can be skipped
        Person['department'] = Person['person_department'].substratum(
                'department')

        jake = self.put('/person', data={'name':'Jake'}).json()
        sales = self.put('/department', data={'name':'Sales'}).json()
        jake_dept = self.put('/person_department', data={
            'person_id':jake['id'], 'department_id':sales['id']}).json()
        self.assertDictContains(jake_dept, {'person_id':1, 'department_id':1})

        # Get Jake's department through jake entry
        sales2 = self.get('/person/1/department').json()
        self.assertEqual(sales, sales2)

        # Same entry can be gotten directly, rather than through substratum
        sales3 = self.get('/person/1/person_department/department').json()
        self.assertEqual(sales, sales3)


