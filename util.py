"""A few handy utils"""
from collection import Collection


def clear_collection( col: Collection ):
    """Delete all keys belonging to a collection"""
    # %%
    keys = col.redis.keys(f"{col.name}/*")
    print( f"Deleting {len(keys)} keys for collection {col.name}")
    if len(keys) > 0:
        col.redis.delete( *keys )
    # %%
