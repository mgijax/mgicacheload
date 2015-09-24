#!/usr/local/bin/python
#
# Any unit tests against logic in go_annot_extensions_display_load.py
#

import sys,os.path
# adjust the path for running the tests locally, so that it can find cacheload (1 dir up)
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import unittest

from go_annot_extensions_display_load import transformProperties

class TransformPropertiesTest(unittest.TestCase):
	"""
	Test the transformProperties() method for generating
		display values and links for GO Annotation Extensions
	"""

	# test nothing! 
	def test_id_not_mapped(self):
		properties = [{'value':'testing', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={},
						    markerIDMap={}
		)
		expected = 'testing'
		
		self.assertEquals(expected, transformed[0]['displayNote'])


	def test_term_id(self):
		properties = [{'value':'CL:0001', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={'CL:0001':'cell ontology term'},
						    markerIDMap={}
		)
		expected = 'cell ontology term'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		
		
	def test_marker_id(self):
		properties = [{'value':'MGI:96677', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={},
						    markerIDMap={'MGI:96677':'Kit'}
		)
		expected = 'Kit'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		
		
		
	def test_term_id_not_mapped(self):
		properties = [{'value':'GO:12345', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={'TERM:1':'test'},
						    markerIDMap={'MARKER:1':'testSymbol'}
		)
		expected = 'GO:12345'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		
		
	def test_marker_id_not_mapped(self):
		properties = [{'value':'MGI:96677', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={'TERM:1':'test'},
						    markerIDMap={'MARKER:1':'testSymbol'}
		)
		expected = 'MGI:96677'
		
		self.assertEquals(expected, transformed[0]['displayNote'])

	### Specific Logical DB criteria ###
	
	def test_PR_id_with_numbers(self):
		properties = [{'value':'PR:001234', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={'PR:001234':'PR term'},
						    markerIDMap={'MARKER:1':'testSymbol'}
		)
		expected = 'PR:001234'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		

	def test_PR_id_with_dash(self):
		properties = [{'value':'PR:QZ434H-1', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={'PR:QZ434H-1':'PR term'},
						    markerIDMap={'MARKER:1':'testSymbol'}
		)
		expected = 'PR:QZ434H-1'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		


if __name__ == '__main__':
	unittest.main()

