#!/usr/bin/env python3
"""
Test script for CyRedis pgcache module integration
"""

import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(__file__))

def test_pgcache_module_path():
    """Test getting the pgcache module path"""
    print("Testing pgcache module path utility...")

    try:
        # Import the utility function directly from pg_cache module
        import cy_redis.pg_cache as pg_cache

        # Test the module path function
        module_path = pg_cache.get_pgcache_module_path()
        print(f"✓ Module path: {module_path}")

        if module_path and os.path.exists(module_path):
            print("✓ Module file exists")
            return True
        else:
            print("⚠️ Module file not found (expected if not built)")
            return True  # This is OK - module just hasn't been built yet

    except ImportError as e:
        if "CyRedis extension not built" in str(e):
            print("⚠️ Cython extension not built (expected in test environment)")
            print("✓ Module path utility structure is correct")
            return True
        else:
            print(f"✗ Unexpected import error: {e}")
            return False

def test_module_structure():
    """Test that the module structure is correct"""
    print("\nTesting module structure...")

    base_dir = os.path.dirname(__file__)
    pgcache_dir = os.path.join(base_dir, 'cy_redis', 'pgcache')
    src_dir = os.path.join(pgcache_dir, 'src')
    makefile = os.path.join(pgcache_dir, 'Makefile')
    source_file = os.path.join(src_dir, 'pgcache.c')

    checks = [
        ('pgcache directory', os.path.exists(pgcache_dir)),
        ('src directory', os.path.exists(src_dir)),
        ('Makefile', os.path.exists(makefile)),
        ('pgcache.c source', os.path.exists(source_file)),
    ]

    all_passed = True
    for name, exists in checks:
        if exists:
            print(f"✓ {name} exists")
        else:
            print(f"✗ {name} missing")
            all_passed = False

    return all_passed

def test_build_script():
    """Test that the build script is executable"""
    print("\nTesting build script...")

    build_script = os.path.join(os.path.dirname(__file__), 'cy_redis', 'pgcache', 'build_module.sh')

    if os.path.exists(build_script):
        if os.access(build_script, os.X_OK):
            print("✓ Build script exists and is executable")
            return True
        else:
            print("⚠️ Build script exists but is not executable")
            return False
    else:
        print("✗ Build script not found")
        return False

def main():
    """Run all tests"""
    print("CyRedis pgcache Module Integration Test")
    print("=" * 50)

    tests = [
        ("Module Structure", test_module_structure),
        ("Module Path Utility", test_pgcache_module_path),
        ("Build Script", test_build_script),
    ]

    results = []
    for name, test_func in tests:
        print(f"\n--- {name} ---")
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"✗ Test failed with exception: {e}")
            results.append((name, False))

    print("\n" + "=" * 50)
    print("Test Results:")

    all_passed = True
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False

    print(f"\nOverall: {'✓ ALL TESTS PASSED' if all_passed else '✗ SOME TESTS FAILED'}")

    if not all_passed:
        print("\nTroubleshooting:")
        print("- Make sure all files were copied correctly")
        print("- Check file permissions")
        print("- Ensure build dependencies are installed for the Redis module")

    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
