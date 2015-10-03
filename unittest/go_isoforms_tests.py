#!/usr/local/bin/python
#
# Any unit tests against logic in go_isoforms_display_load.py
#

import sys,os.path
# adjust the path for running the tests locally, so that it can find cacheload (1 dir up)
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import unittest

from go_isoforms_display_load import transformProperties

class TransformPropertiesTest(unittest.TestCase):
	"""
	Test the transformProperties() method for generating
		display values and links for GO Isoforms
	"""
	
	def setUp(self):
		"""
		Create a map of fake url types
		"""
		# These are false urls for testing only
		self.urlMap = {'EMBL':'http://fake.embl.org/@@@@',
					'Protein Ontology':'http://fake.pro.org/@@@@',
					'UniProt':'http://fake.uniprot.org/@@@@',
					'RefSeq':'http://fake.refseq.org/@@@@'
					}

	def test_id_not_mapped(self):
		properties = [{'value':['testing'], '_evidenceproperty_key':1}]
		transformed = transformProperties(properties)
		expected = 'testing'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		
		
	def test_EMBL_id_link(self):
		properties = [{'value':['EMBL:AA1234'], '_evidenceproperty_key':1}]
		transformed = transformProperties(properties, self.urlMap)
		expected = '\\\\Link(http://fake.embl.org/AA1234|EMBL:AA1234|)'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		
		
	def test_NCBI_id_link(self):
		"""
		treated same as RefSeq
		"""
		properties = [{'value':['NCBI:NP_1234'], '_evidenceproperty_key':1}]
		transformed = transformProperties(properties, self.urlMap)
		expected = '\\\\Link(http://fake.refseq.org/NP_1234|NCBI:NP_1234|)'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		
		
	def test_PR_id_link(self):
		properties = [{'value':['PR:001234'], '_evidenceproperty_key':1}]
		transformed = transformProperties(properties, self.urlMap)
		expected = '\\\\Link(http://fake.pro.org/001234|PR:001234|)'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		
		
	def test_RefSeq_id_link(self):
		properties = [{'value':['RefSeq:XP_1234'], '_evidenceproperty_key':1}]
		transformed = transformProperties(properties, self.urlMap)
		expected = '\\\\Link(http://fake.refseq.org/XP_1234|RefSeq:XP_1234|)'
		
		self.assertEquals(expected, transformed[0]['displayNote'])
		
	def test_UnitProtKB_id_link(self):
		properties = [{'value':['UniProtKB:P1234'], '_evidenceproperty_key':1}]
		transformed = transformProperties(properties, self.urlMap)
		expected = '\\\\Link(http://fake.uniprot.org/P1234|UniProtKB:P1234|)'
		
		self.assertEquals(expected, transformed[0]['displayNote'])

		


if __name__ == '__main__':
	unittest.main()

