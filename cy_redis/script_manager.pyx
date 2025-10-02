# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""
High-Performance Lua Script Manager - Optimized Cython implementation
"""

import json
import hashlib
import time
from typing import Dict, List, Any, Optional, Union
from concurrent.futures import ThreadPoolExecutor

# Import our optimized Redis client
from cy_redis.cy_redis_client import CyRedisClient

# Optimized Lua Script Manager
cdef class CyLuaScriptManager:
    """
    High-performance Lua script manager with caching, versioning, and optimization.
    """

    cdef CyRedisClient redis
    cdef str namespace
    cdef dict loaded_scripts  # script_name -> sha
    cdef dict script_versions  # script_name -> version
    cdef dict script_sources   # script_name -> source_code
    cdef dict script_metadata  # script_name -> metadata
    cdef ThreadPoolExecutor executor

    def __cinit__(self, CyRedisClient redis_client, str namespace="scripts"):
        self.redis = redis_client
        self.namespace = namespace
        self.loaded_scripts = {}
        self.script_versions = {}
        self.script_sources = {}
        self.script_metadata = {}
        self.executor = ThreadPoolExecutor(max_workers=2)

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)

    cdef str _compute_script_hash(self, str script):
        """Compute SHA1 hash of script for caching"""
        return hashlib.sha1(script.encode('utf-8')).hexdigest()

    cpdef str register_script(self, str name, str script, str version="1.0.0",
                             dict metadata=None):
        """
        Register a script with versioning and metadata.
        Returns the script SHA.
        """
        cdef str existing_version, existing_sha, computed_sha

        # Check if script already exists with same version
        existing_version = self.redis.hget(f"{self.namespace}:versions", name)
        if existing_version == version:
            # Script already loaded, get SHA
            existing_sha = self.redis.hget(f"{self.namespace}:shas", name)
            if existing_sha:
                self.loaded_scripts[name] = existing_sha
                self.script_versions[name] = version
                self.script_sources[name] = script
                if metadata:
                    self.script_metadata[name] = metadata
                return existing_sha

        # Load new script
        try:
            computed_sha = self.redis.script_load(script)

            # Store metadata in Redis
            self.redis.hset(f"{self.namespace}:shas", name, computed_sha)
            self.redis.hset(f"{self.namespace}:versions", name, version)
            self.redis.hset(f"{self.namespace}:sources", name, script)
            self.redis.hset(f"{self.namespace}:metadata", name, json.dumps(metadata or {}))

            # Update local cache
            self.loaded_scripts[name] = computed_sha
            self.script_versions[name] = version
            self.script_sources[name] = script
            if metadata:
                self.script_metadata[name] = metadata

            return computed_sha
        except Exception as e:
            raise Exception(f"Failed to register script {name}: {e}")

    cpdef object execute_script(self, str name, list keys=None, list args=None):
        """Execute a registered script by name"""
        if name not in self.loaded_scripts:
            raise Exception(f"Script {name} not registered")

        cdef str sha = self.loaded_scripts[name]
        return self.redis.evalsha(sha, keys or [], args or [])

    cpdef dict get_script_info(self, str name):
        """Get comprehensive information about a registered script"""
        if name not in self.loaded_scripts:
            return {}

        cdef str sha = self.loaded_scripts[name]
        cdef list exists_result = self.redis.script_exists([sha])
        cdef bint cached = bool(exists_result[0]) if exists_result else False

        cdef dict metadata = {}
        cdef str metadata_json = self.redis.hget(f"{self.namespace}:metadata", name)
        if metadata_json:
            try:
                metadata = json.loads(metadata_json)
            except:
                pass

        return {
            'name': name,
            'sha': sha,
            'version': self.script_versions.get(name),
            'source_length': len(self.script_sources.get(name, "")),
            'cached': cached,
            'metadata': metadata,
            'last_accessed': time.time()
        }

    cpdef dict list_scripts(self):
        """List all registered scripts with their information"""
        cdef dict scripts = {}
        for name in self.loaded_scripts.keys():
            scripts[name] = self.get_script_info(name)
        return scripts

    cpdef dict reload_all_scripts(self):
        """Reload all registered scripts (useful after Redis restart)"""
        cdef dict results = {}
        for name, source in self.script_sources.items():
            try:
                new_sha = self.redis.script_load(source)
                self.loaded_scripts[name] = new_sha
                self.redis.hset(f"{self.namespace}:shas", name, new_sha)
                results[name] = f"Reloaded: {new_sha}"
            except Exception as e:
                results[name] = f"Failed: {e}"
        return results

    cpdef dict validate_script(self, str script):
        """Validate script syntax and check for issues"""
        try:
            # Try to load the script
            cdef str sha = self.redis.script_load(script)

            # Check if it's already cached
            cdef list exists = self.redis.script_exists([sha])

            # Try to execute with dummy data to check for runtime errors
            cdef object test_result = None
            try:
                test_result = self.redis.evalsha(sha, [], [])
            except Exception as e:
                runtime_error = str(e)
            else:
                runtime_error = None

            return {
                'valid': True,
                'sha': sha,
                'cached': bool(exists[0]) if exists else False,
                'runtime_error': runtime_error,
                'length': len(script)
            }

        except Exception as e:
            return {
                'valid': False,
                'error': str(e),
                'length': len(script)
            }

    cpdef dict cleanup_scripts(self, bint confirm=False):
        """Clean up script cache and metadata"""
        if not confirm:
            return {
                'error': 'Confirmation required. Set confirm=True to proceed.',
                'scripts_to_remove': list(self.loaded_scripts.keys())
            }

        # Flush script cache
        self.redis.script_flush()

        # Clear local cache
        self.loaded_scripts.clear()
        self.script_versions.clear()
        self.script_sources.clear()
        self.script_metadata.clear()

        # Remove metadata from Redis (get all script names first)
        cdef str pattern = f"{self.namespace}:*"
        # Note: SCAN would be better but for simplicity we'll use known keys
        cdef list keys_to_delete = [
            f"{self.namespace}:shas",
            f"{self.namespace}:versions",
            f"{self.namespace}:sources",
            f"{self.namespace}:metadata"
        ]

        for key in keys_to_delete:
            self.redis.delete(key)

        return {
            'status': 'cleanup_complete',
            'scripts_removed': len(self.loaded_scripts)
        }

    cpdef object load_script_from_file(self, str name, str filepath, str version="1.0.0"):
        """Load a script from file"""
        try:
            with open(filepath, 'r') as f:
                script_content = f.read()
            return self.register_script(name, script_content, version, {'filepath': filepath})
        except Exception as e:
            raise Exception(f"Failed to load script from file {filepath}: {e}")

    cpdef dict get_script_stats(self):
        """Get comprehensive statistics about script usage"""
        cdef dict stats = {
            'total_scripts': len(self.loaded_scripts),
            'namespace': self.namespace,
            'cache_info': {},
            'performance': {}
        }

        # Cache information
        cdef int cached_count = 0
        cdef int total_size = 0
        for name, sha in self.loaded_scripts.items():
            cdef list exists = self.redis.script_exists([sha])
            if exists and exists[0]:
                cached_count += 1
            total_size += len(self.script_sources.get(name, ""))

        stats['cache_info'] = {
            'cached_scripts': cached_count,
            'total_scripts': len(self.loaded_scripts),
            'cache_hit_rate': cached_count / len(self.loaded_scripts) if self.loaded_scripts else 0,
            'total_source_size': total_size
        }

        return stats

    cpdef dict atomic_load_and_test(self, str name, str script, str version="1.0.0",
                                   dict test_cases=None, dict metadata=None):
        """
        Atomically load, test, and register a script with function mapping.

        This operation is atomic - either the script loads, tests pass, and functions
        are mapped, or the entire operation fails and nothing is registered.

        Args:
            name: Script name
            script: Lua script source
            version: Script version
            test_cases: Dict of test cases to run
            metadata: Additional metadata

        Returns:
            Dict with results including mapped functions
        """
        cdef dict result = {
            'success': False,
            'name': name,
            'version': version,
            'sha': None,
            'functions': {},
            'tests_passed': [],
            'tests_failed': [],
            'error': None
        }

        try:
            # Step 1: Validate script syntax
            validation = self.validate_script(script)
            if not validation['valid']:
                result['error'] = f"Script validation failed: {validation['error']}"
                return result

            # Step 2: Load script temporarily (don't register yet)
            temp_sha = self.redis.script_load(script)

            # Step 3: Run test cases if provided
            if test_cases:
                for test_name, test_data in test_cases.items():
                    try:
                        keys = test_data.get('keys', [])
                        args = test_data.get('args', [])
                        expected = test_data.get('expected', None)

                        actual = self.redis.evalsha(temp_sha, keys, args)

                        if expected is not None and actual != expected:
                            result['tests_failed'].append({
                                'test': test_name,
                                'expected': expected,
                                'actual': actual
                            })
                        else:
                            result['tests_passed'].append(test_name)

                    except Exception as e:
                        result['tests_failed'].append({
                            'test': test_name,
                            'error': str(e)
                        })

                # If any tests failed, abort
                if result['tests_failed']:
                    # Clean up temporary script
                    try:
                        self.redis.script_flush()
                    except:
                        pass
                    result['error'] = f"Test failures: {len(result['tests_failed'])}"
                    return result

            # Step 4: Register the script (now that validation passed)
            final_sha = self.register_script(name, script, version, metadata)
            result['sha'] = final_sha

            # Step 5: Create function mappings
            result['functions'] = self._create_function_mappings(name, script, final_sha)

            result['success'] = True
            return result

        except Exception as e:
            result['error'] = str(e)
            # Clean up on failure
            try:
                self.redis.script_flush()
            except:
                pass
            return result

    cdef dict _create_function_mappings(self, str name, str script, str sha):
        """Create function mappings from script analysis"""
        cdef dict functions = {}

        # Analyze script for function patterns (basic analysis)
        if 'rate_limiter' in name.lower():
            functions['check_rate'] = lambda keys, args: self.redis.evalsha(sha, keys, args)
            functions['is_allowed'] = lambda keys, args: self.redis.evalsha(sha, keys, args)
        elif 'distributed_lock' in name.lower():
            functions['acquire_lock'] = lambda keys, args: self.redis.evalsha(sha, keys, args)
            functions['release_lock'] = lambda keys, args: self.redis.evalsha(sha, keys, args)
            functions['extend_lock'] = lambda keys, args: self.redis.evalsha(sha, keys, ['extend'] + args)
        elif 'smart_cache' in name.lower():
            functions['cache_get'] = lambda keys, args: self.redis.evalsha(sha, keys, ['GET'] + args)
            functions['cache_set'] = lambda keys, args: self.redis.evalsha(sha, keys, ['SET'] + args)
            functions['cache_stats'] = lambda keys, args: self.redis.evalsha(sha, keys, ['STATS'] + args)
        elif 'job_queue' in name.lower():
            functions['push_job'] = lambda keys, args: self.redis.evalsha(sha, keys, ['PUSH'] + args)
            functions['pop_job'] = lambda keys, args: self.redis.evalsha(sha, keys, ['POP'] + args)
            functions['complete_job'] = lambda keys, args: self.redis.evalsha(sha, keys, ['COMPLETE'] + args)
            functions['fail_job'] = lambda keys, args: self.redis.evalsha(sha, keys, ['FAIL'] + args)
            functions['queue_stats'] = lambda keys, args: self.redis.evalsha(sha, keys, ['STATS'] + args)

        # Always provide generic execute function
        functions['execute'] = lambda keys, args: self.redis.evalsha(sha, keys, args)

        return functions

    cpdef dict atomic_deploy_scripts(self, dict scripts_config):
        """
        Atomically deploy multiple scripts with testing and function mapping.

        Args:
            scripts_config: Dict of script_name -> config
                config = {
                    'script': str,           # Lua script
                    'version': str,          # Version
                    'test_cases': dict,      # Test cases
                    'metadata': dict         # Additional metadata
                }

        Returns:
            Dict with deployment results
        """
        cdef dict results = {
            'success': True,
            'deployed_scripts': [],
            'failed_scripts': [],
            'function_mappings': {}
        }

        cdef dict temp_state = {}  # Track what we've loaded so far

        try:
            for script_name, config in scripts_config.items():
                script = config['script']
                version = config.get('version', '1.0.0')
                test_cases = config.get('test_cases')
                metadata = config.get('metadata', {})

                # Deploy this script
                deploy_result = self.atomic_load_and_test(
                    script_name, script, version, test_cases, metadata
                )

                if deploy_result['success']:
                    results['deployed_scripts'].append(script_name)
                    results['function_mappings'][script_name] = deploy_result['functions']
                    temp_state[script_name] = deploy_result
                else:
                    results['failed_scripts'].append({
                        'name': script_name,
                        'error': deploy_result['error'],
                        'failed_tests': deploy_result['tests_failed']
                    })
                    results['success'] = False
                    break  # Stop on first failure

            # If all succeeded, we're done
            if results['success']:
                return results
            else:
                # Rollback - remove all scripts we loaded
                for script_name in results['deployed_scripts']:
                    try:
                        self.redis.hdel(f"{self.namespace}:shas", script_name)
                        self.redis.hdel(f"{self.namespace}:versions", script_name)
                        self.redis.hdel(f"{self.namespace}:sources", script_name)
                        self.redis.hdel(f"{self.namespace}:metadata", script_name)
                    except:
                        pass  # Best effort cleanup

                # Clear local state
                for script_name in results['deployed_scripts']:
                    if script_name in self.loaded_scripts:
                        del self.loaded_scripts[script_name]
                    if script_name in self.script_versions:
                        del self.script_versions[script_name]
                    if script_name in self.script_sources:
                        del self.script_sources[script_name]
                    if script_name in self.script_metadata:
                        del self.script_metadata[script_name]

                # Flush script cache to clean up
                try:
                    self.redis.script_flush()
                except:
                    pass

                return results

        except Exception as e:
            results['success'] = False
            results['error'] = str(e)
            return results


# Python wrapper for the optimized script manager
class OptimizedLuaScriptManager:
    """
    High-performance Lua script manager with Cython backend.
    Drop-in replacement for LuaScriptManager with better performance.
    """

    def __init__(self, redis_client=None, namespace: str = "scripts"):
        if redis_client is None:
            from optimized_redis import OptimizedRedis
            redis_client = OptimizedRedis()

        # Use the Cython backend if available, otherwise fallback
        try:
            self._impl = CyLuaScriptManager(redis_client.client, namespace)
        except AttributeError:
            # Fallback to Python implementation
            from redis_wrapper import LuaScriptManager
            self._impl = LuaScriptManager(redis_client, namespace)

    def register_script(self, name: str, script: str, version: str = "1.0.0",
                       metadata: dict = None) -> str:
        return self._impl.register_script(name, script, version, metadata)

    def execute_script(self, name: str, keys: Optional[List[str]] = None,
                      args: Optional[List[str]] = None) -> Any:
        return self._impl.execute_script(name, keys, args)

    def get_script_info(self, name: str) -> dict:
        return self._impl.get_script_info(name)

    def list_scripts(self) -> dict:
        return self._impl.list_scripts()

    def reload_all_scripts(self) -> dict:
        return self._impl.reload_all_scripts()

    def validate_script(self, script: str) -> dict:
        return self._impl.validate_script(script)

    def cleanup_scripts(self, confirm: bool = False) -> dict:
        return self._impl.cleanup_scripts(confirm)

    def load_script_from_file(self, name: str, filepath: str, version: str = "1.0.0"):
        return self._impl.load_script_from_file(name, filepath, version)

    def get_script_stats(self) -> dict:
        return self._impl.get_script_stats()

    def atomic_load_and_test(self, name: str, script: str, version: str = "1.0.0",
                           test_cases: dict = None, metadata: dict = None) -> dict:
        """Atomically load, test, and register a script with function mapping."""
        return self._impl.atomic_load_and_test(name, script, version, test_cases, metadata)

    def atomic_deploy_scripts(self, scripts_config: dict) -> dict:
        """Atomically deploy multiple scripts with testing and function mapping."""
        return self._impl.atomic_deploy_scripts(scripts_config)
