from dictapi.cpapi import API
from dictapi.dictapi import NoRead, NoWrite, LastModified, COLLECTION_SIZE
from dictapi.test_dictapi import BaseTest
from functools import partial
import cherrypy
import json
import os
import psycopg2
import requests
import unittest


class BaseCherryPy(BaseTest):

    def setUp(self):
        super().setUp()

        # This API comes from the CherryPy API module
        self.api = API(self.conn)

        cherrypy.config.update({
            'log.screen':False,
            'log.access_file':'',
            'log.error_file':''
            })
        self.app = cherrypy.tree.mount(self.api, '/api',
                config=self.api.generate_config())
        cherrypy.engine.start()

        # uncomment for troubleshooting
        #cherrypy.config.update({'log.screen':True})


    def __request(self, method, path, data=None, params=None, headers=None):
        request = requests.Request(method, 'http://127.0.0.1:8080/api'+path,
                data=data, params=params, headers=headers)
        request = request.prepare()
        response = requests.Session().send(request)
        return response


    def delete(self, *a, **kw):  return self.__request('DELETE', *a, **kw)
    def get(self, *a, **kw):     return self.__request('GET', *a, **kw)
    def head(self, *a, **kw):    return self.__request('HEAD', *a, **kw)
    def options(self, *a, **kw): return self.__request('OPTIONS', *a, **kw)
    def post(self, *a, **kw):    return self.__request('POST', *a, **kw)
    def put(self, *a, **kw):     return self.__request('PUT', *a, **kw)


    def assertResponse(self, expected_code, response, expected_entry):
        self.assertEqual(expected_code, response.status_code)
        entry = response.json()
        self.assertDictContains(entry, expected_entry)
        return entry


    def assertError(self, expected_code, error):
        self.assertEqual(expected_code, error.status_code)
        self.assertIn('error', error.json())




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
        error = self.get('/person', params={'id':1})
        self.assertError(404, error)

        # Incorrect primary keys also fails
        error = self.get('/person', params={'foo':1, 'bar':2})
        self.assertEqual(400, error.status_code)

        # Getting Jake's 2 reference is invalid
        self.put('/person', data={'name':'Jake'})
        error = self.get('/person/1/2')
        self.assertError(400, error)

        # No person with ID 2
        error = self.get('/person/2/3')
        self.assertError(404, error)


    def test_reference(self):
        """
        The person table references the department table using a join table.
        """
        # Setup the references for each table
        self.reference_pd()

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

        # The "department" reference is set, but not gotten
        jake = self.get('/person/1').json()
        self.assertNotIn('department', jake)


    def test_modify(self):
        jake = self.put('/person', data={'name':'Jake'}).json()
        self.assertIn('password_hash', jake)
        self.assertNotEqual(jake['last_modified'], None)

        # Enable LastModified to track changes
        self.api.person.apitable.PUT.modify(LastModified, 'last_modified')
        original_modified = jake['last_modified']

        # Disallow reading of password_hash
        self.api.person.apitable.GET.modify(NoRead, 'password_hash')
        self.api.person.apitable.PUT.modify(NoRead, 'password_hash')
        jake = self.get('/person', params={'id':1}).json()
        self.assertIsInstance(jake, dict)
        self.assertNotIn('password_hash', jake)

        # Write to password_hash
        jake2 = self.put('/person', data={'id':1, 'name':'Jake',
            'password_hash':'foobar'}).json()
        self.assertEqual(jake['id'], jake2['id'])
        self.assertNotIn('password_hash', jake2)
        self.assertEqual(
                self.api.person.table.get_one(1)['password_hash'],
                'foobar')
        self.conn.rollback()

        # Disallow writing to password_hash
        self.api.person.apitable.PUT.modify(NoWrite, 'password_hash')
        error = self.put('/person', data={'id':1, 'password_hash':'error'})
        self.assertError(400, error)

        # Writing is still possible
        frank = self.put('/person', data={'id':1, 'name':'Frank'}).json()
        self.assertDictContains(frank, {'id':1, 'name':'Frank'})

        self.assertGreater(frank['last_modified'], original_modified)


    def test_head(self):
        jake = self.put('/person', data={'name':'Jake'}).json()
        head = self.head('/person', params={'id':1})
        self.assertEqual(head.status_code, 200)
        # Resulting JSON is an empty dict, which raises an error when decoded
        self.assertRaises(json.decoder.JSONDecodeError, head.json)


    def test_delete(self):
        # Too many primary keys
        error = self.delete('/person/1/2')
        self.assertError(400, error)

        # No entry, yet
        error = self.delete('/person/1')
        self.assertError(404, error)

        # Create and delete Jake
        self.put('/person', data={'name':'Jake'})
        response = self.delete('/person/1')
        self.assertEqual(response.status_code, 200)

        # Create Jake with referenced column, delete will fail
        self.put('/person', data={'name':'Jake'})
        self.put('/department', data={'name':'Sales'})
        self.put('/person_department', data={'person_id':2, 'department_id':1})
        error = self.delete('/person/2')
        self.assertError(400, error)


    def test_get_pagination(self):
        # department is referenced through person_department
        self.reference_pd()

        response = self.get('/person')
        self.assertEqual(404, response.status_code)

        names = ('Jake', 'Phil', 'Bob', 'Steve', 'Alice', 'Frank')*4
        for name in names:
            self.api.dictdb['person'](name=name).flush()
        self.conn.commit()

        # Get all Persons inserted
        response = self.get('/person', headers={'Range':'1-'})
        self.assertEqual(200, response.status_code, msg=response.content)
        persons = response.json()
        self.assertEqual(len(persons), COLLECTION_SIZE)
        last_id = 0
        for person, name in zip(persons, names):
            self.assertEqual(last_id+1, person['id'])
            last_id = person['id']
            self.assertDictContains(person, {'name':name})
            self.assertNotIn('department', person)

        response = self.get('/person', params={'page':2},
                headers={'Range':'21-40'})
        self.assertEqual(200, response.status_code)
        persons = response.json()
        self.assertEqual(len(persons), 4)
        for person, name in zip(persons, names[-4:]):
            self.assertDictContains(person, {'name':name})


    def test_errors(self):
        # No person
        error = self.get('/person', params={'id':1})
        self.assertError(404, error)

        # Bad ID value
        error = self.get('/person', params={'id':'foo'})
        self.assertError(400, error)
        error = self.put('/person', data={'id':'foo'})
        self.assertError(400, error)

        # Bad column name
        error = self.get('/person', params={'foo':'bar'})
        self.assertError(400, error)


    def test_options(self):
        expected_options = ['DELETE', 'GET', 'HEAD', 'OPTIONS', 'PUT']
        response = self.options('/person')
        self.assertEqual(response.headers['Allow'],
                ', '.join(expected_options))
        self.assertEqual(response.json(), expected_options)

        del self.api.person.HEAD
        expected_options = ['DELETE', 'GET', 'OPTIONS', 'PUT']
        response = self.options('/person')
        self.assertEqual(response.headers['Allow'],
                ', '.join(expected_options))
        self.assertEqual(response.json(), expected_options)


