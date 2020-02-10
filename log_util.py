"""get logging funs"""

from logging import getLogger, StreamHandler

LOGGERS = {}
# %%


def debug_log_fun( name: str, stream=None ):
    """Get debug member function from named logger"""
    return log_fun( name, "debug", stream )


def info_log_fun(name: str, stream=None):
    """Get debug member function from named logger"""
    return log_fun(name, "info", stream)


def log_fun( name: str, typ: str, stream=None ):
    """Get debug member function from named logger"""
    logger = getLogger( name )

    # avoid adding a handler again if there is already one using this stream
    if stream is not None:
        already = [ h2 for h2 in logger.handlers if h2.stream == stream ]
        if len(already) == 0:
            logger.addHandler( StreamHandler(stream) )

    return { "info": logger.info,
             "debug": logger.debug }[typ]
