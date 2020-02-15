"""Parse search expressions"""

from arpeggio.cleanpeg import ParserPEG

# Non standard precedence!
# OR binds more tightly than AND!
# ( x AND y OR z AND a OR b ) -> ( x AND (y OR z) AND (a OR b) )

peg_grammar = """
lit_number = r'\d+(\.\d*)?|\d+'
lit_str = r'[A-Za-z0-9-]+' / r'"[A-Za-z0-9 ]*"'
lit_bool = 'true' / 'false'
lit_val = lit_number / lit_str / lit_bool

range = lit_number 'TO' lit_number
fld_name = r'[A-Za-z_][A-Za-z0-9_]*'
cmp_operator = '=' / '<=' / '>=' / '<' / '>'
 
match_expr = fld_name ':' ( range / lit_val )
num_filter_expr =  fld_name cmp_operator lit_number
tag_expr = lit_str
filter_expr = match_expr / num_filter_expr / tag_expr

filter_clause = ('NOT' filter_expr) / filter_expr / '(' expr ')'

term = filter_clause ("OR" filter_clause)*
expr = term ("AND" term)*

search_expr = expr EOF
"""
# %%


def interactive_testing():
    # %%
    parser = ParserPEG(peg_grammar, root_rule_name='search_expr')
    # %%
    parser.parse('fld:4 AND fld1:5')
    # %%
    parser.parse('fld:1 TO 2')
    # %%
    parser.parse('NOT fld:true')
    # %%
    parser.parse('price < 10 AND (category:Book OR NOT category:Ebook)')
    # %%
    parser.parse('country:Colombia OR country:USA')
    # %%
    d = parser.parse('country:Colombia OR country:USA')
    # %%
    parser.parse('NOT country:venezuela')
    # %%
    parser.parse('(category:Book OR category:Ebook) AND NOT author:"JK Rowling"')
    # %%
    parser.parse("f1:v1 OR f2:v2")
    # %%
    parser.parse("NOT(f1:v1 OR f2:v2)")
    # %%
    parser.parse('x AND y OR z AND a OR b')
    # %%
    parser.parse( 'x OR y AND z OR a AND b' )
    # %%
    parser.parse('(x OR y) AND (z OR a) AND b')
    # %%
