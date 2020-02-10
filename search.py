"""Core classes to implement search filters"""

import sys
from typing import Union, List, Set
import abc

import os
import uuid
import datetime as dt

from redis.client import Pipeline

import common as com
from common import Key, Field
from collection import Collection
from importlib import reload

from log_util import info_log_fun, debug_log_fun
from logging import DEBUG, getLogger

l_dbg = debug_log_fun("search", sys.stdout )  # pylint: disable=invalid-name
l_info = info_log_fun("search", sys.stdout )  # pylint: disable=invalid-name
getLogger("search").setLevel(DEBUG)

# %%

LiteralVal = Union[str, int, float]


class SearchContext:
    """Package collection search is carried out on, together with pipeline and
    temporary key generating funcionality"""

    def __init__(self, col: Collection, pipe: Pipeline ):
        self.col = col
        self.pipe = pipe
        self.tmp_keys = []
        # %%
        prefix0 = f"{uuid.getnode()}-{os.getpid()}-{dt.datetime.now().timestamp()}"

        self.tmp_key_prefix = str(hash( prefix0 ))

    def gen_key(self) -> str:
        """generate first key and record it in tmp_keys"""
        next_i = len(self.tmp_keys)
        key = f"t/{self.tmp_key_prefix}:{next_i}"
        self.tmp_keys.append( key )
        return key


class Expr( abc.ABC ):
    """Abstract base class for all expressions"""
    def eval(self, ctx: SearchContext) -> Key:
        """run a search within this context"""
        pass


class FacetEq( Expr ):
    """Represents a comparison such as f('name') == 'Teo' """
    def __init__(self, fld: Field, val: LiteralVal):
        self.fld = fld
        self.val = val

    def eval(self, ctx: SearchContext) -> Key:
        """For facet fields get the key containing set of docs with this value in the field"""
        col = ctx.col
        if self.fld in col.cfg.facet_flds:
            ret = com.key_facet_fld_val( col.name, self.fld, self.val )
            l_dbg(f"{self} : ret = {ret}")
            return ret
        else:
            raise RuntimeError("FacetEq search not implemented for non facet flds")

    def __str__(self) -> str:
        return f"{self.fld} == {self.val}"


class ContainsToken( Expr ):
    """Represents a search   doc['fld'] contains 'word' """
    def __init__(self, tok: LiteralVal):
        self.tok = tok

    def eval(self, ctx: SearchContext) -> Key:
        """For facet fields get the key containing set of docs with this value in the field"""
        col = ctx.col
        ret = com.key_token( col.name, self.tok )
        l_dbg(f"{self} : ret = {ret}")
        return ret

    def __str__(self) -> str:
        return f"contains('{self.tok}')"


class ContainsTokens( Expr ):
    """Represents a search   doc['fld'] contains 'word' """
    def __init__(self, tokens: List[LiteralVal]):
        self.toks = tokens
        self.expr = And( *[ ContainsToken(tok) for tok in tokens ] )

    def eval(self, ctx: SearchContext) -> Key:
        """Run search"""
        return self.expr.eval( ctx )

    def __str__(self) -> str:
        return f"doc contains all of {self.toks}"


class ContainsApprox( Expr ):
    """Will match a document that contains this word among its tokens
    maybe even with typos"""
    def __init__(self, word: LiteralVal, max_typos=2 ):
        self.word = str(word)

        patterns = { word }

        for i in range( max_typos ):
            extends: List[Set[str]] = []
            for pat in patterns:
                extends.append( patterns1typo( pat ) )

            for ext in extends:
                patterns = patterns.union( ext )

        self.patterns = patterns

    def eval(self, ctx: SearchContext) -> Key:
        red = ctx.pipe
        # col_name = ctx.col.name
        tokens_key = f"{ctx.col.name}/text_tokens"

        ret = []
        for pat in self.patterns:
            for tok in red.sscan_iter(tokens_key, match=pat, count=100000):
                ret.append( tok )

        return ret

# %%


def patterns1typo( word: str ) -> Set[str]:
    """Patterns allowing one typo"""
    ret = []
    word_l = len( word )
    for pos in range(word_l + 1):
        ret.append( word[:pos] + '?' + word[pos:] )
        if pos < word_l:
            ret.append( word[:pos] + '?' + word[pos+1:])

    return set(ret)
    # %%


class Or( Expr ):
    """Represents disjunction of several expressions"""
    def __init__( self, arg1: Union[List, Expr], *args: Expr ):
        if isinstance( arg1, list ):
            assert len(args) >= 2
            self.children = arg1
        elif isinstance( arg1, Expr):
            assert len(args) >= 1
            self.children = [arg1] + list(args)
        else:
            raise ValueError(f"arg1: {type(arg1)}")

    def eval(self, ctx: SearchContext):
        """Carry out set union of Redis sets and store result in temporary key"""
        pipe = ctx.pipe

        key = self.children[0].eval( ctx )
        for child in self.children[1:]:
            key1 = child.eval( ctx )
            key2 = ctx.gen_key()
            l_dbg( f"{key2} <- {key} U {key1}" )
            pipe.sunionstore( key2, key, key1 )
            key = key2

        return key


class And( Expr ):
    """Represents disjunction of several expressions"""
    def __init__( self, arg1: Union[List, Expr], *args: Expr ):
        if isinstance(arg1, list):
            assert len(args) >= 2
            self.children = arg1
        else:
            assert len(args) >= 1
            self.children = [arg1] + list(args)

    def eval(self, ctx: SearchContext):
        """Carry out set intersection of Redis sets and store result in temporary key"""
        pipe = ctx.pipe

        key = self.children[0].eval( ctx )
        for child in self.children[1:]:
            key1 = child.eval( ctx )
            key2 = ctx.gen_key()
            l_dbg(f"{key2} <- {key} & {key1}")
            pipe.sinterstore( key2, key, key1 )
            key = key2

        return key


def run_search( col: Collection, search_expr: Expr ) -> List[Key]:
    """run search on a collection based on an expression"""
    # %%
    red = col.redis
    # with red.pipeline() as pipe:
    ctx = SearchContext(col, col.redis )
    key = search_expr.eval( ctx )
    # ret = pipe.execute()
    l_dbg( f"key={key}")

    return col.redis.smembers(key)


def interactive_testing( col: Collection):
    # %%
    # noinspection PyUnresolvedReferences
    runfile("search.py")  # pylint: disable=undefined-variable

    expr = ContainsApprox("cobre", max_typos=2)

    red = col.redis
    # with col.redis.pipeline() as pipe:
    # for i in range(3): red.ping()
    ctx = SearchContext(col, col.redis)
    reload( com )
    com.timeit( lambda: expr.eval( ctx ) )
    # pipe.execute()
    # %%
    script = """
    local extend = function( t1, t2 )
        for _, el in ipairs(t2) do
            table.insert( t1, el )
        end
        return t1
    end
    
    local search = function (key, pat)
       
       local cur = '0' 
       local res = {}
       while 1 do
         local r = redis.call('sscan', key, cur, 'match', pat, 'count', '25000' )
         
         extend( res, r[2] )
         if r[1] == '0' then
            break
         end
         cur = r[1]
       end
       return res
    end
    
    local res = {}
    for _, arg in ipairs( ARGV ) do
        res = extend( res, search(KEYS[1], arg) )
    end
    
    return res 
    """

    t0 = dt.datetime.now()
    ret = col.redis.eval( script, 1, f'{col.name}/text_tokens', *expr.patterns)
    t1 = dt.datetime.now()

    print( (t1 - t0).total_seconds() * 1000, len(ret) )
    # %%
