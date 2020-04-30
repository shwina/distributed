from .utils_comm import collection_types, WrappedKey
from .utils import tokey
from dask.optimization import SubgraphCallable


def unpack_remotedata(object o, byte_keys=False, object myset=None):
    """ Unpack WrappedKey objects from collection

    Returns original collection and set of all found WrappedKey objects

    Examples
    --------
    >>> rd = WrappedKey('mykey')
    >>> unpack_remotedata(1)
    (1, set())
    >>> unpack_remotedata(())
    ((), set())
    >>> unpack_remotedata(rd)
    ('mykey', {WrappedKey('mykey')})
    >>> unpack_remotedata([1, rd])
    ([1, 'mykey'], {WrappedKey('mykey')})
    >>> unpack_remotedata({1: rd})
    ({1: 'mykey'}, {WrappedKey('mykey')})
    >>> unpack_remotedata({1: [rd]})
    ({1: ['mykey']}, {WrappedKey('mykey')})

    Use the ``byte_keys=True`` keyword to force string keys

    >>> rd = WrappedKey(('x', 1))
    >>> unpack_remotedata(rd, byte_keys=True)
    ("('x', 1)", {WrappedKey('('x', 1)')})
    """
    if myset is None:
        myset = set()
        out = unpack_remotedata(o, byte_keys, myset)
        return out, myset

    typ = type(o)

    if typ is tuple:
        if not o:
            return o
        if type(o[0]) is SubgraphCallable:
            sc = o[0]
            futures = set()
            dsk = {
                k: unpack_remotedata(v, byte_keys, futures) for k, v in sc.dsk.items()
            }
            args = tuple(unpack_remotedata(i, byte_keys, futures) for i in o[1:])
            if futures:
                myset.update(futures)
                futures = (
                    tuple(tokey(f.key) for f in futures)
                    if byte_keys
                    else tuple(f.key for f in futures)
                )
                inkeys = sc.inkeys + futures
                return (
                    (SubgraphCallable(dsk, sc.outkey, inkeys, sc.name),)
                    + args
                    + futures
                )
            else:
                return o
        else:
            return tuple(unpack_remotedata(item, byte_keys, myset) for item in o)
    if typ in collection_types:
        if not o:
            return o
        outs = [unpack_remotedata(item, byte_keys, myset) for item in o]
        return typ(outs)
    elif typ is dict:
        if o:
            values = [unpack_remotedata(v, byte_keys, myset) for v in o.values()]
            return dict(zip(o.keys(), values))
        else:
            return o
    elif issubclass(typ, WrappedKey):  # TODO use type is Future
        k = o.key
        if byte_keys:
            k = tokey(k)
        myset.add(o)
        return k
    else:
        return o

