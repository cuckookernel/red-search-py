"""Testing"""

import datetime as dt
from importlib import reload
import redis

import search as sch
import indexing as idx
from common import f
import collection
from collection import Collection, CollectionConfig
from util import clear_collection
import common as com

reload(collection)
reload( idx )
reload( sch )
# %%


def cocktail_col( redis_conn ):
    """Example cocktail collection"""
    cfg = CollectionConfig(name='cocktails',
                           id_fld='id',
                           facet_flds=['ingredients', 'main_color'],
                           text_flds=['description', 'name', 'seo_title'],
                           number_flds=['num_ingredients'],
                           stop_words='a el los la las es y o'.split()  )

    # %%
    return Collection(redis_conn).configure(cfg)


def populate_cocktails( col: Collection ):
    """Put some cocktails in the collection"""
    docs = [{'id': 1,
             "description": "acidic and highly alcohólico",
             "ingredients": ["vodka", "rum"],
             "main_color": "transparent",
             "num_ingredients": 4},
            {'id': 2,
             "description": "sweet and bitter",
             "ingredients": ["cointreau", "rum"],
             "main_color": "white",
             "num_ingredients": 6},
            ]
    # %%
    for doc in docs:
        collection.index_document(col, doc)


def create_and_populte_quijote( redis_conn ):
    """Load the full text of el quijote and store as independent lines"""
    # %%

    with open( "/home/teo/_data/red-search/quijote.txt", "rt", encoding="iso-8859-1") as f_in:
        lines = f_in.readlines()

    print( len(lines) )
    docs = [ {"text": line, "id": i, "par_num": i} for i, line in enumerate(lines) ]
    # %%
    cfg = CollectionConfig(name='qxt',
                           id_fld='id',
                           text_flds=['text'],
                           facet_flds=[],
                           number_flds=['par_num'],
                           stop_words="el la los las de a es".split(" "))

    col = Collection(redis_conn).configure(cfg)

    # %%
    clear_collection(col)
    # %%
    com.timeit( lambda: collection.index_documents(col, docs, batch_size=100) )
    # %%


def interactive_testing( ):
    """Interactive testing"""
    # %%
    # noinspection PyUnresolvedReferences
    runfile("test.py")  # pylint: disable=undefined-variable
    redis_conn = redis.Redis(host='localhost', port=6379, db=0)
    # %%
    col = cocktail_col( redis_conn )
    # %%
    populate_cocktails( col )
    # %%
    sexpr1 = sch.FacetEq(f("ingredients"), "vodka")
    sexpr2 = sch.FacetEq(f("ingredients"), "rum")
    # %%
    sch.run_search( col, sexpr1 )
    # %%
    or_expr = sch.Or( sexpr1, sexpr2 )
    results = sch.run_search( col, or_expr )
    print( results )
    # %%
    and_expr = sch.And( sexpr1, sexpr2 )
    print( sch.run_search( col, and_expr ) )
    # %%
    print(sch.run_search(col, sch.And( sch.FacetEq(f("ingredients"), "vodka"),
                                       sch.FacetEq(f("ingredients"), "cointreau") )))


def testing_qxt( col ):
    """testing with el quikote"""
    # %%
    reload( sch )
    reload( idx )
    expr = sch.ContainsToken("cobre")
    print( sch.run_search( col, expr ) )
    col.redis.ping()
    # %%
    col.redis.sscan( b'qxt/text_tokens', count=1000000 )
