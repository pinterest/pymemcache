"""
Backwards compatibility with the older serialization api previously
provided by this module.
"""
from . import codecs

_SERDE = codecs.Serde()

python_memcache_serializer = _SERDE.from_python
python_memcache_deserializer = _SERDE.to_python
