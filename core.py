"""Core classes and functions"""

from typing import List, Dict, Any, Optional
import json
import redis
from redis import Redis

# %%
Doc = Dict[str, Any]

class CollectionConfig:
    """Configuration for a collection"""
    def __init__(self, name: str,
                 id_fld: str, facet_flds: List[str], text_flds: List[str],
                 number_flds: List[str]):

        self.name = name
        self.id_fld = id_fld
        self.text_flds = text_flds
        self.facet_flds = facet_flds
        self.number_flds = number_flds


class Collection:
    """basic collection methods"""
    def __init__(self, redis_conn: Redis):
        self.redis = redis_conn
        self.cfg: Optional[CollectionConfig] = None
        self.id_fld = None
        self.name = None

    def configure(self, cfg: CollectionConfig):
        """set the config"""
        self.cfg = cfg
        self.id_fld = self.cfg.id_fld
        self.name = self.cfg.name

        return self

    def x_id(self, doc: Doc) -> str:
        """xtract the id from a document"""
        return str(doc[self.id_fld])


def load_document( col: Collection, doc: Doc ):
    """Push a document into the index"""
    # %%
    # doc_id = doc[ col.id_fld ]
    doc_id = col.x_id(doc)
    red = col.redis
    red.hset( f'{col.name}/docs', doc_id, json.dumps(doc) )

    for fld in col.cfg.text_flds:
        if fld not in doc:
            continue

        text = doc[fld]

        tokens = tokenize(text)
        if len(tokens) > 0:
            red.sadd( f'{col.name}/all_tokens', *tokens )

        for tk in tokens:
            red.sadd( f'{col.name}/token_docs/{tk}', doc_id )
    # %%


def tokenize(text) -> List[str]:
    """Produce a list of tokens from a text"""
    # TODO: make it better, remove accents, stopwords
    tokens = [ t for t in text.lower().split() if t != '' ]
    return tokens

    # %%

def interactive_test():
    # %%
    redis_conn = redis.Redis(host='localhost', port=6379, db=0)
    # %%

    # noinspection PyUnresolvedReferences
    runfile('core.py')

    cfg = CollectionConfig( name='cocktails',
                            id_fld='id',
                            facet_flds=['ingredients', 'main_color'],
                            text_flds=['description', 'name', 'seo_title'],
                            number_flds=['num_ingredients'] )

    # %%
    col = Collection( redis_conn ).configure( cfg )
    # %%
    doc = { 'id': 1, "description": "acidic and highly alcohólico" }
    # %%




