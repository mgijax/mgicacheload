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

	def setUp(self):
		"""
		Create a map of fake url types
		"""
		# These are false urls for testing only
		self.urlMap = {
					'Cell Ontology':'http://fake.cl.org/@@@@',
					'Ensembl Gene Model':'http://fake.ensembl.org/@@@@',
					'Protein Ontology':'http://fake.pro.org/@@@@',
					'UniProt':'http://fake.uniprot.org/@@@@',
					}

	# test nothing! 
	def test_id_not_mapped(self):
		properties = [{'value':'testing', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={},
						    markerIDMap={}
		)
		expected = 'testing'
		
		self.assertEquals(expected, transformed[0]['displayNote'])


	def test_CL_id(self):
		properties = [{'value':'CL:0001', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={'CL:0001':'cell ontology term'},
						    markerIDMap={},
						    providerLinkMap=self.urlMap
		)
		expected = '\\\\Link(http://fake.cl.org/CL_0001|cell ontology term|)'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		
		
	def test_GO_id(self):
		properties = [{'value':'GO:12345', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={'GO:12345':'test GO term'},
						    markerIDMap={},
						    providerLinkMap=self.urlMap
		)
		expected = '\\\\Acc(GO:12345|test GO term|)'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		
		
	def test_marker_id(self):
		properties = [{'value':'MGI:96677', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={},
						    markerIDMap={'MGI:96677':'Kit'},
						    providerLinkMap=self.urlMap
		)
		expected = '\\\\Marker(MGI:96677|Kit|)'
		
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
	
	def test_Ensembl_id_with_numbers(self):
		properties = [{'value':'ENSEMBL:ENSMUG12345', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={'TERM:1':'test term'},
						    markerIDMap={'MARKER:1':'testSymbol'},
						    providerLinkMap=self.urlMap
		)
		expected = '\\\\Link(http://fake.ensembl.org/ENSMUG12345|ENSEMBL:ENSMUG12345|)'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		
	
	def test_PR_id_with_numbers(self):
		properties = [{'value':'PR:001234', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={'PR:001234':'PR term'},
						    markerIDMap={'MARKER:1':'testSymbol'},
						    providerLinkMap=self.urlMap
		)
		expected = '\\\\Link(http://fake.pro.org/PR:001234|PR:001234|)'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		

	def test_PR_id_with_dash(self):
		properties = [{'value':'PR:QZ434H-1', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={'PR:QZ434H-1':'PR term'},
						    markerIDMap={'MARKER:1':'testSymbol'},
						    providerLinkMap=self.urlMap
		)
		expected = '\\\\Link(http://fake.pro.org/PR:QZ434H-1|PR:QZ434H-1|)'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		
		
		
	def test_UnitProtKB_id_with_numbers(self):
		properties = [{'value':'UniProtKB:P1234', '_evidenceproperty_key':1}]
		transformed = transformProperties(properties,
						    termIDMap={'TERM:1':'test term'},
						    markerIDMap={'MARKER:1':'testSymbol'},
						    providerLinkMap=self.urlMap
		)
		expected = '\\\\Link(http://fake.uniprot.org/P1234|UniProtKB:P1234|)'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		


if __name__ == '__main__':
	unittest.main()

