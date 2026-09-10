# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

"""
Module capability detection for Redis and Valkey

Redis and Valkey load different module sets, and where both ecosystems ship a
module for the same feature the command surfaces are not always identical:
valkey-search indexes TAG, NUMERIC and VECTOR fields through FT.CREATE and
FT.SEARCH but implements none of the RediSearch aggregation, suggestion or
dictionary commands. A server answers a command it does not know with a bare
``unknown command`` error, so the module wrappers route their calls through
:func:`execute_module_command`, which turns that error into a diagnosis naming
the module that provides the command and the servers that carry it.
"""

from typing import Dict, List, Optional, Set


class ModuleUnavailableError(RuntimeError):
    """Raised when the server has no module providing the command."""

    def __init__(self, command: str, message: str):
        self.command = command
        super().__init__(message)


# Command prefix -> (feature, module on Redis, module on Valkey, names the
# module reports itself under in MODULE LIST).
_PROVIDERS = {
    'JSON': ('JSON documents', 'RedisJSON (Redis Stack)', 'valkey-json (valkey-bundle)',
             frozenset(['rejson', 'json'])),
    'FT': ('search indexes', 'RediSearch (Redis Stack)', 'valkey-search (valkey-bundle)',
           frozenset(['search', 'searchlight'])),
    'GRAPH': ('graph queries', 'RedisGraph, end-of-life since Redis Stack 7.4', None,
              frozenset(['graph'])),
    'AI': ('model serving', 'RedisAI', None, frozenset(['ai'])),
    'TS': ('time series', 'RedisTimeSeries (Redis Stack)', None, frozenset(['timeseries'])),
    'BF': ('Bloom filters', 'RedisBloom (Redis Stack)', 'valkey-bloom (valkey-bundle)',
           frozenset(['bf'])),
    'CF': ('Cuckoo filters', 'RedisBloom (Redis Stack)', None, frozenset(['bf'])),
    'CMS': ('count-min sketches', 'RedisBloom (Redis Stack)', None, frozenset(['bf'])),
    'TOPK': ('top-k sketches', 'RedisBloom (Redis Stack)', None, frozenset(['bf'])),
}

# Commands valkey-search implements; the rest of FT.* is RediSearch-only.
VALKEY_SEARCH_COMMANDS = frozenset([
    'FT._LIST',
    'FT.CREATE',
    'FT.DROPINDEX',
    'FT.INFO',
    'FT.SEARCH',
])


cpdef set module_names(object conn):
    """Names of the modules loaded on the server, lowercased."""
    cdef set names = set()
    reply = conn.execute_command(['MODULE', 'LIST'])
    if not reply:
        return names

    for module in reply:
        if isinstance(module, dict):
            name = module.get('name')
        elif isinstance(module, (list, tuple)):
            fields = {module[i]: module[i + 1] for i in range(0, len(module) - 1, 2)}
            name = fields.get('name') or fields.get(b'name')
        else:
            name = None

        if name is None:
            continue
        if isinstance(name, (bytes, bytearray)):
            name = name.decode('utf-8', 'replace')
        names.add(name.lower())

    return names


cpdef bint supports_command(object conn, str command):
    """Whether the server knows ``command``.

    ``COMMAND INFO`` answers with a null entry for commands the server cannot
    run, which covers both missing modules and modules that only implement part
    of a command family.
    """
    reply = conn.execute_command(['COMMAND', 'INFO', command])
    if not reply:
        return False
    return reply[0] is not None


cdef bint _is_unknown_command(object error):
    text = str(error).lower()
    return 'unknown command' in text or 'unknown subcommand' in text


cdef object _explain(object conn, str command):
    cdef str family = command.split('.', 1)[0].upper()
    provider = _PROVIDERS.get(family)
    if provider is None:
        return None

    feature, redis_module, valkey_module, module_ids = provider
    cdef bint is_valkey = _server_is_valkey(conn)
    cdef bint family_loaded = bool(module_ids & module_names(conn))
    if family_loaded:
        parts = ["%s is not implemented by the module serving %s on this server."
                 % (command, feature)]
    else:
        parts = ["%s is not available on this server; no module providing %s is loaded."
                 % (command, feature)]

    if is_valkey and family_loaded and family == 'FT':
        parts.append(
            "%s implements only %s; this command needs %s."
            % (valkey_module, ', '.join(sorted(VALKEY_SEARCH_COMMANDS)), redis_module)
        )
    elif family_loaded:
        parts.append("A newer build of the module may add it.")
    elif is_valkey and valkey_module is None:
        parts.append("Valkey has no module providing it; %s carries it on Redis." % redis_module)
    elif is_valkey:
        parts.append("Load %s to enable it." % valkey_module)
    else:
        parts.append("Load %s to enable it." % redis_module)

    return ' '.join(parts)


cdef bint _server_is_valkey(object conn):
    try:
        info = conn.execute_command(['INFO', 'server'])
    except Exception:
        return False
    if isinstance(info, (bytes, bytearray)):
        info = info.decode('utf-8', 'replace')
    if not isinstance(info, str):
        return False
    return 'valkey_version:' in info or 'server_name:valkey' in info


cpdef object execute_module_command(object conn, list args):
    """Run a module command, reporting a missing module rather than a bare error."""
    try:
        return conn.execute_command(args)
    except ModuleUnavailableError:
        raise
    except Exception as error:
        if not _is_unknown_command(error):
            raise
        command = args[0] if args else ''
        if isinstance(command, (bytes, bytearray)):
            command = command.decode('utf-8', 'replace')
        explanation = _explain(conn, command)
        if explanation is None:
            raise
        raise ModuleUnavailableError(command, explanation) from error
