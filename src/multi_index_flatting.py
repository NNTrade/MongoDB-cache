from pandas import MultiIndex, Index


def flating(multiindex: MultiIndex, tuple_split: str = "|"):
  return multiindex.map(lambda el: tuple_split.join(el))


def multiindexing(index: Index, tuple_split: str = "|"):
  return index.map(lambda el: tuple(map(str, el.split(tuple_split))))
