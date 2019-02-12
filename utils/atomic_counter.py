import itertools


# https://stackoverflow.com/a/27062830/1580227
class AtomicCounter:
    def __init__(self):
        self._counter = itertools.count()
        # init to 0
        next(self._counter)

    def increment(self, increment=1):
        assert increment > 0
        return [next(self._counter) for _ in range(0, increment)][-1]
