"""Core functions for indexing"""
from typing import List, Set, TypeVar

import json
from redis import Redis
from redis.client import Pipeline

from common import ( Doc, Scalar, key_facet_fld_val, key_token, key_numeric_fld,
                     CollectionConfig, is_scalar, is_number, as_list, x_id )
import re

T_ = TypeVar("T_")



def index_text( pipe: Pipeline, cfg: CollectionConfig,  doc_id: str, text: str):
    """Index text from text field"""
    tokens = tokenize(text, cfg.transl_tbl, cfg.stop_words)

    if len(tokens) > 0:
        pipe.sadd(f'{cfg.name}/text_tokens', *tokens)

    for tok in tokens:
        pipe.sadd( key_token( cfg.name, tok), doc_id)


def tokenize(text: str, trans_tabl: str, stop_words: Set[str]) -> List[str]:
    """Produce a list of tokens from a text"""
    text1 = text.lower().translate( trans_tabl )
    # print("text1 = ", text1 )
    text2 = re.sub('[^a-z0-9]', ' ', text1)
    # print("text2 = ", text2 )
    tokens = [ tok for tok in text2.split(" ") if (tok != '' and tok not in stop_words) ]

    return tokens
    # %%


def index_facet( red: Redis, col_name: str, doc_id: str, fld: str, val: Scalar ):
    """Index the fact that doc has a facet value in given field"""
    red.sadd(key_facet_fld_val( col_name, fld, val), doc_id )
    red.sadd(f'{col_name}/doc_facets/{doc_id}', f'f:{fld}/v:{val}')


def index_numeric( red: Redis, col_name: str, doc_id: str, fld: str, val: float ):
    """Index the fact that doc has a facet value in given field"""
    red.zadd(key_numeric_fld( col_name, fld), { doc_id: val } )
    red.sadd(f'{col_name}/doc_num/{doc_id}', f'n:{val}')


def index_document_pipe( pipe: Pipeline, cfg: CollectionConfig, doc: Doc ):
    """Push a document into the index"""
    # doc_id = doc[ col.id_fld ]
    doc_id = x_id(doc, cfg.id_fld)

    pipe.hset( f'{cfg.name}/docs', doc_id, json.dumps(doc) )

    for fld in cfg.text_flds:
        if fld in doc:
            text = doc[fld]
            index_text( pipe, cfg, doc_id, text)

    for fld in cfg.facet_flds:
        if fld not in doc:
            continue

        for val in as_list( doc, fld ):
            assert is_scalar(val), f"Found non scalar value ({val}) in field '{fld}' of " \
                                   f"document with id {doc_id}"

            index_facet( pipe, cfg.name, doc_id, fld, val )

    for fld in cfg.number_flds:
        if fld not in doc:
            continue

        for val in as_list(doc, fld):
            if val is None:
                continue
            assert is_number(val), f"Found non numeric value ({val}) in field '{fld}' of " \
                                   f"document with id {doc_id}"

            index_numeric(pipe, cfg.name, doc_id, fld, val)
