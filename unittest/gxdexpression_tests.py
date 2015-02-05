#
# Any unit tests against logic in gxdexpression.py
#

import sys,os.path
# adjust the path for running the tests locally, so that it can find cacheload (1 dir up)
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import unittest

import gxdexpression

class GxdExpressionCacheTest(unittest.TestCase):
	

	# test expressed flag
	def test_computeExpressed_yes(self):
		"""
		check all possible strengths that
		map to a yes expressed flag
		"""
		yesStrengths = ['Present','Ambiguous',
			'Trace','Weak','Moderate',
			'Strong','Very Strong']
		for yesStrength in yesStrengths:
			results = [{'strength': yesStrength}]
			expressed = gxdexpression.computeExpressedFlag(results)
			self.assertEquals(1, expressed)

	def test_computeExpressed_no(self):
		"""
		check all possible strengths that
		map to a no expressed flag
		"""
		noStrengths = ['Absent','Not Applicable']
		for noStrength in noStrengths:
			results = [{'strength': noStrength}]
			expressed = gxdexpression.computeExpressedFlag(results)
			self.assertEquals(0, expressed)

	def test_computeExpressed_many_yes(self):
		results = [{'strength':'Present'}, {'strength':'Strong'}]
		expressed = gxdexpression.computeExpressedFlag(results)
		self.assertEquals(1, expressed)

	def test_computeExpressed_many_no(self):
		results = [{'strength':'Absent'}, {'strength':'Absent'}]
		expressed = gxdexpression.computeExpressedFlag(results)
		self.assertEquals(0, expressed)

	def test_computeExpressed_many_clash(self):
		results = [{'strength':'Absent'}, {'strength':'Present'},
				 {'strength':'Absent'}]
		expressed = gxdexpression.computeExpressedFlag(results)
		self.assertEquals(1, expressed)


	# test isForGxd
	def test_isForGxd_yes_assaytypes(self):
		"""
		Test all assay types that map to 
		yes isforgxd flag
		"""
		yesTypes = [-2,-1,1,2,3,4,5,6,7,8,9]
		for yesType in yesTypes:
			results = [{'_assaytype_key': yesType}]
			isForGxd = gxdexpression.computeIsForGxd(results)
			self.assertEquals(1, isForGxd)

	def test_isForGxd_no_assaytypes(self):
		"""
		Test all assay types that map to 
		no isforgxd flag
		"""
		noTypes = [10,11]
		for noType in noTypes:
			results = [{'_assaytype_key': noType}]
			isForGxd = gxdexpression.computeIsForGxd(results)
			self.assertEquals(0, isForGxd)

	# test isrecombinase
	def test_isRecombinase_yes_assaytypes(self):
		"""
		Test all assay types that map to 
		yes isrecombinase flag
		"""
		yesTypes = [10,11]
		for yesType in yesTypes:
			results = [{'_assaytype_key': yesType, 'reportergene': None}]
			isRecombinase = gxdexpression.computeIsRecombinase(results)
			self.assertEquals(1, isRecombinase)

	def test_isRecombinase_no_assaytypes(self):
		"""
		Test all assay types that map to 
		no isrecombinase flag
		"""
		noTypes = [-2,-1,1,2,3,4,5,6,7,8,9]
		for noType in noTypes:
			results = [{'_assaytype_key': noType, 'reportergene': None}]
			isRecombinase = gxdexpression.computeIsRecombinase(results)
			self.assertEquals(0, isRecombinase)

	def test_isRecombinase_cre_reporter(self):
		results = [{'_assaytype_key': 9, 'reportergene': 'Cre'}]
		isRecombinase = gxdexpression.computeIsRecombinase(results)
		self.assertEquals(1, isRecombinase)

	def test_isRecombinase_cre_reporter_wrong_type(self):
		results = [{'_assaytype_key': 1, 'reportergene': 'Cre'}]
		isRecombinase = gxdexpression.computeIsRecombinase(results)
		self.assertEquals(0, isRecombinase)

	def test_isRecombinase_flp_reporter(self):
		results = [{'_assaytype_key': 9, 'reportergene': 'FLP'}]
		isRecombinase = gxdexpression.computeIsRecombinase(results)
		self.assertEquals(1, isRecombinase)

	def test_isRecombinase_flp_reporter_wrong_type(self):
		results = [{'_assaytype_key': 1, 'reportergene': 'FLP'}]
		isRecombinase = gxdexpression.computeIsRecombinase(results)
		self.assertEquals(0, isRecombinase)


	# test hasImage
	def test_hasimage_yes(self):
		results = [{'_imagepane_key':1, '_image_key':1, 'image_xdim':1}]
		hasImage = gxdexpression.computeHasImage(results)
		self.assertEquals(1, hasImage)

	def test_hasimage_noxdim(self):
		results = [{'_imagepane_key':1, '_image_key':1, 'image_xdim':None}]
		hasImage = gxdexpression.computeHasImage(results)
		self.assertEquals(0, hasImage)

	def test_hasimage_no_image_key(self):
		results = [{'_imagepane_key':1, '_image_key':None, 'image_xdim':1}]
		hasImage = gxdexpression.computeHasImage(results)
		self.assertEquals(0, hasImage)

	def test_hasimage_no_imagepane_key(self):
		results = [{'_imagepane_key':0, '_image_key':1, 'image_xdim':1}]
		hasImage = gxdexpression.computeHasImage(results)
		self.assertEquals(0, hasImage)

	def test_hasimage_one_of_many(self):
		results = [{'_imagepane_key':None, '_image_key':None, 'image_xdim':None},
			{'_imagepane_key':1, '_image_key':1, 'image_xdim':1},
			{'_imagepane_key':None, '_image_key':None, 'image_xdim':None}]
		hasImage = gxdexpression.computeHasImage(results)
		self.assertEquals(1, hasImage)
		

if __name__ == '__main__':
	unittest.main()

