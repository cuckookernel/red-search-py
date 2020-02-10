"""Core classes and functions

Redis stored information

key template     | type | key    | contents / value  | functions
{col}/docs       | hash | doc_id | json docs         | load_document
{col}/text_tokens | set  |        | text tokens from all docs | index_text
{col}/docs/t:{tk} | set |    | doc_ids that contain  token {tk} in some text field |
{col}/docs/f:{fld}/v:{val} | set |  | doc_ids that contain {val} in field {fld}
{col_name}/doc_facets/{doc_id}' | set |  Set of 'f:{fld}/v:{val}'  for a given doc_id

"""
# TODO: aproximate search of tokens
# TODO: search numeric fields
# TODO: way to update a document
# TODO: in index document make all validation first then commit

from typing import List, Dict, Optional, TypeVar
from redis import Redis
from common import Doc, DocId, CollectionConfig, batches_from_list
import indexing as idx

# %%
T_ = TypeVar("T_")



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
        """extract the id from a document"""
        return str(doc[self.id_fld])

    def index_document(self, doc: Doc):
        """index a document"""
        return index_document( self, doc )

    def get_all_docs(self) -> Dict[DocId, Doc]:
        """get dict of { doc_id -> Doc }"""
        return get_all_docs( self )


def index_document( col: Collection, doc: Doc ):
    """Index a single document in a single transaction"""
    with col.redis.pipeline() as pipe:
        idx.index_document_pipe( pipe, col.cfg, doc )
        pipe.execute()


def index_documents( col: Collection, docs: List[Doc], batch_size=1000 ):
    """insert documents in batches"""
    for batch in batches_from_list(docs, batch_size=batch_size):
        with col.redis.pipeline() as pipe:
            for doc in batch:
                idx.index_document_pipe( pipe, col.cfg, doc )
            pipe.execute()


def get_all_docs( col: Collection ) -> Dict:
    """get all docs in collection as dict"""
    # %%
    return col.redis.hgetall( f"{col.name}/docs" )
    # %%
