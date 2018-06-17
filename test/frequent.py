import random
import unittest
import collections

from streaming import frequent

def load_tests(_, tests, __):
    import doctest
    tests.addTests(doctest.DocTestSuite(frequent))
    return tests

class SelectTests(object):

    class Cases(unittest.TestCase):

        def select(self, sequence, k):
            raise NotImplementedError()

        def test_select(self):
            data = [random.randint(0, 32) for _ in xrange(128)]
            truth = sorted(data)
            with self.assertRaises(ValueError):
                self.select(data, -1)
            with self.assertRaises(ValueError):
                self.select(data, len(data))
            for k in xrange(len(data)):
                self.assertEqual(truth[k], self.select(data, k))

class PySelectTests(SelectTests.Cases):

    def select(self, sequence, k):
        # pylint: disable=E1123
        return frequent.select(sequence, k, python=True)

class SketchTests(object):

    class Cases(unittest.TestCase):

        def create(self, **kwargs):
            raise NotImplementedError()

        def test_perfect(self):
            sketch = self.create(k=1024)
            values = {i: 0 for i in range(1024)}
            for _ in xrange(1 << 12):
                key = random.randint(0, len(values) - 1)
                val = random.randint(0, 1 << 20)
                sketch.update(key, val)
                values[key] += val
            for key, (lo, hi) in sketch.iteritems():
                self.assertEqual(lo, hi)
                self.assertEqual(lo, values[key])

        def test_pareto(self):
            sketch = self.create(k=128)
            values = collections.defaultdict(int)
            for _ in xrange(1 << 15):
                key = int(random.paretovariate(0.7))
                val = random.randint(0, 1 << 20)
                sketch.update(key, val)
                values[key] += val
            self.assertGreater(len(values), 128)
            truth = sorted(
                values.iteritems(),
                reverse=True,
                key=lambda item: item[1]
            )
            estimate = sorted(
                sketch.iteritems(),
                reverse=True,
                key=lambda item: item[1][0]
            )
            for (tk, tv), (ek, (elo, _)) in zip(truth, estimate)[:10]:
                self.assertEqual(tk, ek)
                self.assertAlmostEqual(tv, elo, delta=0.15 * tv)

        def test_top_k(self):
            sketch = self.create(k=5)
            sketch.update('foo', 100)
            self.assertEqual(sketch['foo'], (100, 100))
            for i in range(5):
                sketch.update(str(i), (i + 1) * 1000)
            self.assertNotIn('foo', sketch)
            self.assertIn('4', sketch)

        def test_merge(self):
            s, o = self.create(), self.create()
            s.update('foo', 100)
            o.update('foo', 100)
            m = s | o
            self.assertEqual(s['foo'], (100, 100))
            self.assertEqual(m['foo'], (200, 200))
            m.update('bar', 100)
            s |= m
            self.assertEqual(s['foo'], (300, 300))
            self.assertEqual(s['bar'], (100, 100))
            self.assertEqual(m['foo'], (200, 200))

class PySketchTests(SketchTests.Cases):

    def create(self, **kwargs):
        return frequent.Sketch(
            # pylint: disable=E1123
            counter=frequent.Counter(python=True, **kwargs)
        )
