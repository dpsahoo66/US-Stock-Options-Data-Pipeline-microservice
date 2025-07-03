# from collector.utils.symbols import symbols
import itertools

def create_batches(items, batch_size):
    iterator = iter(items)
    return iter(lambda: list(itertools.islice(iterator, batch_size)), [])
