from pandas import MultiIndex, Index


def multiindex_to_index(multiindex: MultiIndex, tuple_split: str = "|"):
  return multiindex.map(lambda el: tuple_split.join(el))


def index_to_multiindexing(index: Index, tuple_split: str = "|"):
  return index.map(lambda el: tuple(map(str, el.split(tuple_split))))
