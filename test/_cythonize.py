import unittest

from streaming._cythonize import cythonize

def load_tests(_, tests, __):
    import doctest
    import streaming._cythonize
    # pylint: disable=W0212
    tests.addTests(doctest.DocTestSuite(streaming._cythonize))
    return tests

def _cyfunction():
    """cydocs"""
    return 'cython'

class _CyClass(object):
    """cydocs"""
    pass

class CythonizeTestCases(unittest.TestCase):

    def test_function(self):
        @cythonize
        def function(positional, keyword=None):  # pylint: disable=W0613
            """documentation"""
            return 'python'

        self.assertEqual(function.__name__, 'function')
        self.assertEqual(function.__doc__, 'documentation')
        self.assertEqual(function(None), 'python')

        @cythonize
        def cyfunction():
            """documentation"""
            return 'python'

        self.assertEqual(cyfunction.__name__, 'cyfunction')
        self.assertEqual(cyfunction.__doc__, 'documentation')
        self.assertEqual(cyfunction(), 'cython')
        # pylint: disable=E1123
        self.assertEqual(cyfunction(python=True), 'python')

    def test_class(self):
        @cythonize
        class Class(object):
            """documentation"""
            pass

        self.assertEqual(Class.__name__, 'Class')
        self.assertEqual(Class.__doc__, 'documentation')
        self.assertTrue(isinstance(Class(), Class))

        @cythonize
        class CyClass(object):
            """documentation"""
            pass

        self.assertEqual(CyClass.__name__, 'CyClass')
        self.assertEqual(CyClass.__doc__, 'documentation')
        self.assertTrue(isinstance(CyClass(), _CyClass))
        self.assertTrue(isinstance(CyClass(python=True), CyClass))
