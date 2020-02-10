"""common classes and functions used throughout"""

from typing import Dict, Any, Union, List, TypeVar
import datetime as dt

Doc = Dict[str, Any]
DocId = int
Scalar = Union[int, str, float]
Key = bytes

T_ = TypeVar("T_")


class Field(str):
    """Represents a field reference in search expressions"""
    pass


def f( name ) -> Field:
    """Shortcut to quickly create a field"""
    return Field(name)


class CollectionConfig:
    """Configuration for a collection"""
    def __init__(self, name: str,
                 id_fld: str, facet_flds: List[str], text_flds: List[str],
                 number_flds: List[str], stop_words: List[str]):

        self.name = name
        self.id_fld = id_fld
        self.text_flds = text_flds
        self.facet_flds = facet_flds
        self.number_flds = number_flds
        self.stop_words = set( stop_words )
        self.transl_tbl = str.maketrans(dict(zip("áéíóúàèìòùñç", "aeiouaeiounc")))


def x_id(doc: Doc, id_fld: str):
    """xtract id form document"""
    return str(doc[id_fld])


def is_scalar( val: Any ) -> bool:
    """Whether val is str, int of float, i.e. a simple value"""
    return isinstance(val, str) or isinstance(val, float) or isinstance(val, int)


def is_number( val: Any ) -> bool:
    """Whether val is int of float, i.e. a simple numberic value"""
    return isinstance(val, float) or isinstance(val, int)


def key_token( col_name: str, tok: str ) -> Key:
    """Redis Key of set containing ids of cocuments that contain this token in any of their
     text fields"""
    return f'{col_name}/docs/t:{tok}'.encode('utf8')


def key_facet_fld_val( col_name: str, fld: str, val: Scalar ) -> Key:
    """Redis Key of set containing doc ids of documents that contain {val} in facet field {fld}"""
    return f'{col_name}/docs/f:{fld}/v:{val}'.encode('utf8')


def key_numeric_fld( col_name: str, fld: str ) -> Key:
    """Redis Key of sorted set containing doc ids of documents and values for the given
     numeric field"""
    return f'{col_name}/docs/n:{fld}'.encode("utf8")


def as_list( doc: Doc, fld: str ):
    """If value of field is list return as is, otherwise return single element list [ doc[fld] ] """
    val0 = doc[fld]

    if isinstance(val0, list):
        return val0
    elif val0 is None:
        return []
    else:
        return [val0]


def batches_from_list(a_list: List[T_], batch_size: int ):
    """Generate successive batchs of at most batch_size elements from a given list"""
    l_len = len(a_list)
    for ndx in range(0, l_len, batch_size):
        yield a_list[ndx:min(ndx + batch_size, l_len)]


def timeit( fun ):
    t0 = dt.datetime.now()
    ret = fun()
    t1 = dt.datetime.now()

    print( "%.3f ms" % ((t1 - t0).total_seconds() * 1000.0) )
    return ret
