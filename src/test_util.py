#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Contains various useful functions for testing."""




import json

_CARS_SCHEMA = """[
  {"name": "Year", "type": "integer", "mode": "required", "encrypt": "none"},
  {"name": "Make", "type": "string", "mode": "required",
  "encrypt": "pseudonym"},
  {"name": "Model", "type": "string", "mode": "required",
  "encrypt": "probabilistic_searchwords"},
  {"name": "Description", "type": "string", "mode": "nullable",
  "encrypt": "searchwords"},
  {"name": "Website", "type": "string", "mode": "nullable",
  "encrypt": "searchwords", "searchwords_separator": "/"},
  {"name": "Price", "type": "float", "mode": "required",
  "encrypt": "probabilistic"},
  {"name": "Invoice_Price", "type": "integer", "mode": "required",
  "encrypt": "homomorphic"},
  {"name": "Holdback_Percentage", "type": "float", "mode": "required",
  "encrypt": "homomorphic"}
]\n"""

_CARS_CSV = (
    '1997,Ford,E350,"ac4, abs, moon","www.ford.com",3000.00,2000,1.2\n'
    '1999,Chevy,"Venture ""Extended Edition""","","www.cheverolet.com",'
    '4900.00,3800,2.3\n'
    '1999,Chevy,"Venture ""Extended Edition, Very Large""","",'
    '"www.chevrolet.com",5000.00,4300,1.9\n'
    '1996,Jeep,Grand Cherokee,"MUST SELL! air, moon roof, loaded",'
    '"www.chrysler.com/jeep/grand-cherokee",4799.00,3950,2.4\n'
)

_JOBS_SCHEMA = """[
  {"name": "kind", "type": "string", "mode": "required", "encrypt": "none"},
  {"name": "fullName", "type": "string", "mode": "required", "encrypt":
  "pseudonym"},
  {"name": "age", "type": "integer", "mode": "required", "encrypt":
  "homomorphic"},
  {"name": "gender", "type": "string", "mode": "nullable", "encrypt":
  "probabilistic_searchwords"},
  {"name": "citiesLived", "type": "record", "mode": "repeated", "fields":
  [
    {"name": "place", "type": "string", "mode": "required", "encrypt":
    "searchwords", "searchwords_separator": "/"},
    {"name": "numberOfYears", "type": "float", "mode": "required", "encrypt":
    "homomorphic"},
    {"name": "job", "type": "record", "mode": "repeated", "fields":
    [
      {"name": "position", "type": "string", "mode": "required", "encrypt":
      "pseudonym"},
      {"name": "yearsPositionHeld", "type": "float", "mode": "required",
      "encrypt": "probabilistic"},
      {"name": "manager", "type": "string", "mode": "repeated", "encrypt":
      "searchwords"},
      {"name": "jobRank", "type": "integer", "mode": "nullable", "encrypt":
      "none"}
    ]}
  ]}
]\n"""

_JOBS_JSON = (
    '{"kind": "person", "fullName": "John Doe", "age": 22, "gender": "Male", '
    '"citiesLived": [{ "place": "Seattle", "numberOfYears": 5.1, '
    '"job": [{"position": "chef", "yearsPositionHeld": 1.2, '
    '"manager": ["Pierre A", "Oscar B", "Charlie C"], "jobRank": 1}, '
    '{"position": "cashier", "yearsPositionHeld": 0.5, "manager": []}]}, '
    '{"place": "Stockholm", "numberOfYears": 6.0, "job": []}]}\n'
    '{"kind": "person", "fullName": "Imaginary Person", "age": 42, '
    '"citiesLived": []}\n'
    '{"kind": "person", "fullName": "Little Child", "age": 2, '
    '"gender": "Female", "citiesLived": [{"place": "Toronto", '
    '"numberOfYears": 2.0, "job": []}]}\n'
)

_PLACES_SCHEMA = """[
  {"name": "kind", "type": "string", "mode": "required", "encrypt": "none"},
  {"name": "fullName", "type": "string", "mode": "required", "encrypt":
  "pseudonym"},
  {"name": "age", "type": "integer", "mode": "required", "encrypt":
  "homomorphic"},
  {"name": "gender", "type": "string", "mode": "nullable", "encrypt":
  "probabilistic_searchwords"},
  {"name": "citiesLived", "type": "record", "mode": "repeated", "fields":
  [
    {"name": "place", "type": "string", "mode": "required", "encrypt":
    "searchwords", "searchwords_separator": "/"},
    {"name": "numberOfYears", "type": "float", "mode": "required", "encrypt":
    "homomorphic"},
    {"name": "lat", "type": "float", "mode": "nullable", "encrypt": "none"},
    {"name": "long", "type": "float", "mode": "nullable", "encrypt": "none"}
  ]},
  {"name": "spouse", "type": "record", "mode": "required", "fields":
  [
    {"name": "spouseName", "type": "string", "mode": "required", "encrypt":
    "pseudonym"},
    {"name": "yearsMarried", "type": "float", "mode": "required", "encrypt":
    "homomorphic"},
    {"name": "spouseAge", "type": "integer", "mode": "required", "encrypt":
    "none"}
  ]}
]\n"""

_PLACES_JSON = (
    '{"kind": "person", "fullName": "John Doe", "age": 22, "gender": "Male", '
    '"citiesLived": [{ "place": "Seattle", "numberOfYears": 5.1}, '
    '{"place": "Stockholm", "numberOfYears": 6.0, '
    '"lat": "1.0", "long": "2.0"}], '
    '"spouse": {"spouseName": "Jane Doe", "yearsMarried": 0.5, '
    '"spouseAge": 23}}\n'
    '{"kind": "person", "fullName": "Jane Austen", "age": 24, '
    '"gender": "Female", "citiesLived": [{"place": "Los Angeles", '
    '"numberOfYears": 0.2}, {"place": "Tokyo", "numberOfYears": 1.2}], '
    '"spouse": {"spouseName": "James Austen", "yearsMarried": 1.5, '
    '"spouseAge": 24}}\n'
    '{"kind": "person", "fullName": "Imaginary Person", "age": 42, '
    '"gender": "Male", "citiesLived": [], "spouse": {'
    '"spouseName": "Another Imaginary Person", "yearsMarried": 0.0, '
    '"spouseAge": 42}}\n'
    '{"kind": "person", "fullName": "Little Child", "age": 2, '
    '"gender": "Female", "citiesLived": [{"place": "Toronto", '
    '"numberOfYears": 2.0}], "spouse": {"spouseName": "Another Little Child", '
    '"yearsMarried": 0.0, "spouseAge": 2}}\n'
)

_TEST_MASTER_KEY = 'GL9pK+nrHIxSGHMgGUxLmQ=='
_TEST_RELATED = '123'


def GetCarsSchema():
  return json.loads(_CARS_SCHEMA)


def GetCarsSchemaString():
  return _CARS_SCHEMA


def GetJobsSchema():
  return json.loads(_JOBS_SCHEMA)


def GetJobsSchemaString():
  return _JOBS_SCHEMA


def GetPlacesSchema():
  return json.loads(_PLACES_SCHEMA)


def GetPlacesSchemaString():
  return _PLACES_SCHEMA


def GetMasterKey():
  return _TEST_MASTER_KEY


def GetRelated():
  return _TEST_RELATED


def GetCarsCsv():
  return _CARS_CSV


def GetJobsJson():
  return _JOBS_JSON


def GetPlacesJson():
  return _PLACES_JSON
