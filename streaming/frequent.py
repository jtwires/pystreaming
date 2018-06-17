"""estimates the frequency of (weighted) items in a stream.

this module implements a constant-sized sketch that tracks the
frequency of the most-prevalent items in a data stream. updates run in
constant amortized time, and estimation accuracy is bounded by the
residual length of the stream (i.e., the length of the stream
excluding the most frequent items). consequently, accuracy is
relatively high for distributions with high skew (like some pareto and
zipf distributions), but relatively low for uniform and normal
distributions.

sketches can also be merged to estimate the frequency of items in the
concatenated streams.

see 'a high-performance algorithm for identifying frequent items in
data streams' by anderson et al. for more details.

"""
import random
import collections

from streaming._cythonize import cythonize

class Sketch(collections.Mapping):
    """frequency sketch"""

    # pylint: disable=W0231

    def __init__(self, k=4096, sample=None, order=None, counter=None):
        """initialize the sketch.

        :param int k: the number of counters to maintain. this
        parameter presents a space/accuracy tradeoff: maintaining
        more counters provides higher accuracy at the cost of higher
        memory utilization.

        :param int sample: the number of counters to sample when
        rebalancing the sketch. this parameter presents a
        time/accuracy tradeoff: sampling more counters increases
        runtime but may provide slightly improved accuracy. a value of
        1024 is recommended for most use cases.

        :param int order: the kth-order statistic of the sampled
        values to use as the rebalance threshold. lower values give
        improved accuracy at the expense of increased runtime (in
        particular, the constant amortized update time is not
        guaranteed for very low orders). a value of `sample / 2`
        (i.e., the median of the sampled values) is recommended for
        most use cases.

        :param Sketch counter: use the provided counter rather than
        creating a new one (used mostly for testing).

        """
        self._cnt = (
            Counter(k=k, sample=sample, order=order)
            if counter is None else
            counter
        )

    def __len__(self):
        """return the running length of the stream.

        >>> s = Sketch()
        >>> len(s)
        0
        >>> s.update('foo', 100)
        >>> len(s)
        100
        >>> s.update('bar', 200)
        >>> len(s)
        300

        """
        return len(self._cnt)

    def __nonzero__(self):
        """return true iff the sketch has observed non-zero items.

        >>> s = Sketch()
        >>> bool(s)
        False
        >>> s.update('foo', 100)
        >>> bool(s)
        True

        """
        return len(self) > 0

    def __getitem__(self, key):
        """return the estimated frequency of a given item.

        note that this will always return an estimate, even for items
        that have not been observed.

        >>> s = Sketch()
        >>> s.update('foo', 100)
        >>> s['foo']
        (100, 100)
        >>> s['bar']
        (0, 0)

        :param key: the item identifier (e.g., a string)
        :returns: the lower and upper bounds of the estimated frequency
        :rtype: a tuple of ints

        """
        return self.frequency(key)

    def __contains__(self, key):
        """return true iff item is an estimated heavy hitter.

        >>> s = Sketch(k=5)
        >>> s.update('foo', 100)
        >>> 'foo' in s
        True
        >>> for i in range(5):
        ...     s.update(str(i), (i + 1) * 1000)
        >>> 'foo' in s
        False
        >>> '4' in s
        True

        """
        return key in self._cnt

    def __iter__(self):
        """iterate the high-frequency items in the stream.

        >>> s = Sketch()
        >>> list(s)
        []
        >>> s.update('foo', 100)
        >>> list(s)
        ['foo']

        """
        return self._cnt.keys()

    def __ior__(self, other):
        """merge `other` (a second frequency sketch) into this one.

        updates the existing sketch with the counts from the second
        sketch.

        >>> s, o = Sketch(), Sketch()
        >>> s.update('foo', 100)
        >>> s['foo']
        (100, 100)
        >>> o.update('foo', 100)
        >>> o.update('bar', 100)
        >>> s |= o
        >>> len(s)
        300
        >>> s['foo']
        (200, 200)
        >>> s['bar']
        (100, 100)

        :param Sketch other: a frequency sketch

        """
        if not isinstance(other, Sketch):
            return NotImplemented
        # pylint: disable=W0212
        self._cnt.merge(other._cnt)
        return self

    def __or__(self, other):
        """return the union of two sketches.

        >>> s, o = Sketch(), Sketch()
        >>> s.update('foo', 100)
        >>> o.update('foo', 100)
        >>> m = s | o
        >>> s['foo']
        (100, 100)
        >>> len(m)
        200
        >>> m['foo']
        (200, 200)

        """
        if not isinstance(other, Sketch):
            return NotImplemented
        # pylint: disable=W0212
        return Sketch(counter=self._cnt.copy().merge(other._cnt))

    def update(self, key, val=1):
        """update the counter for a given item."""
        return self._cnt.update(key, val)

    def frequency(self, key):
        """return the estimated frequency for a given item."""
        return self._cnt.frequency(key)

    def clear(self):
        """reset the counter"""
        self._cnt.clear()

@cythonize
class Counter(object):
    """pure python frequency counter implementation"""

    def __init__(self, k=4096, sample=None, order=None):
        sample = min(k, 1024) if sample is None else sample
        order = (sample / 2) if order is None else order
        if sample < 0 or sample > k:
            raise ValueError('sample rate out of range')
        if order < 0 or order >= sample:
            raise ValueError('order out of range')
        self.size = k
        self.sample = sample
        self.order = order
        self.length = 0
        self.offset = 0
        self.index = {}

    def __len__(self):
        return self.length

    def __contains__(self, key):
        return key in self.index

    def update(self, key, val=1):
        if not val:
            return
        if val < 0:
            raise ValueError('value out of range')

        self.length += val
        if key in self.index:
            self.index[key] += val
        elif len(self.index) < self.size:
            self.index[key] = val
        else:
            c = self._decrement()
            if val > c:
                assert len(self.index) < self.size
                self.index[key] = val - c

    def frequency(self, key):
        val = self.index.get(key, 0)
        return (val, val + self.offset)

    def keys(self):
        return self.index.iterkeys()

    def merge(self, other):
        if not isinstance(other, Counter):
            return NotImplemented
        length = self.length + other.length
        for key, val in other.index.iteritems():
            self.update(key, val)
        self.length = length
        self.offset += other.offset
        return self

    def clear(self):
        self.index.clear()
        self.length = 0
        self.offset = 0

    def copy(self):
        copy = Counter(
            k=self.size, sample=self.sample, order=self.order
        )
        copy.length, copy.offset = self.length, self.offset
        copy.index.update(self.index)
        return copy

    def _decrement(self):
        zeros, threshold = [], select(
            random.sample(self.index.values(), self.sample),
            self.order,
        )
        for key, val in self.index.iteritems():
            if val <= threshold:
                zeros.append(key)
            else:
                self.index[key] = val - threshold
        for key in zeros:
            self.index.pop(key)
        self.offset += threshold
        return threshold

@cythonize
def select(sequence, k):
    """return the (zero-indexed) kth order statistic of `sequence`

    NB: this function may update the order of values in `sequence`.

    :param list sequence: a list (or tuple or sequence) of numbers
    :param int k: the desired order statistic (0 <= k < len(sequence))
    :returns: the value of the kth element in the sequence

    """
    if not isinstance(sequence, (list, tuple, collections.Sequence)):
        sequence = list(sequence)
    if k < 0 or k >= len(sequence):
        raise ValueError('k value out of range')

    def partition(seq, lo, hi):
        i, j, v = lo, hi + 1, seq[lo]

        while True:
            while True:
                i += 1
                if i == hi or seq[i] >= v:
                    break

            while True:
                j -= 1
                if j == lo or seq[j] <= v:
                    break

            if i >= j:
                break

            seq[i], seq[j] = seq[j], seq[i]

        seq[lo], seq[j] = seq[j], seq[lo]
        return j

    def qselect(seq, lo, hi, pivot):
        while hi > lo:
            idx = partition(seq, lo, hi)
            if idx == pivot:
                return seq[idx]
            if idx > pivot:
                hi = idx - 1
            else:
                lo = idx + 1
        return seq[pivot]

    return qselect(sequence, 0, len(sequence) - 1, k)
