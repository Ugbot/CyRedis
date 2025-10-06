#!/usr/bin/env python3
"""
Test runner for CyRedis worker coordination features.
Can run individual test files or all worker coordination tests.
"""

import sys
import subprocess
import argparse
import os

# Add the tests directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

def run_tests(test_files=None, verbose=True, coverage=False):
    """Run the worker coordination tests."""

    if test_files is None:
        # Run all worker coordination tests
        test_files = [
            'integration/test_worker_coordination.py',
            'unit/test_worker_coordination_unit.py'
        ]

    cmd = ['python', '-m', 'pytest']

    if verbose:
        cmd.append('-v')

    if coverage:
        cmd.extend(['--cov=cy_redis', '--cov-report=term-missing'])

    # Add test markers
    cmd.extend(['-m', 'worker_coordination or unit'])

    # Add test files
    for test_file in test_files:
        cmd.append(os.path.join('tests', test_file))

    print(f"Running tests: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=os.path.dirname(os.path.dirname(__file__)))

    return result.returncode == 0

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Run CyRedis worker coordination tests')
    parser.add_argument('--integration', action='store_true',
                       help='Run only integration tests')
    parser.add_argument('--unit', action='store_true',
                       help='Run only unit tests')
    parser.add_argument('--coverage', action='store_true',
                       help='Enable coverage reporting')
    parser.add_argument('--quiet', action='store_true',
                       help='Reduce output verbosity')

    args = parser.parse_args()

    if args.integration:
        test_files = ['integration/test_worker_coordination.py']
    elif args.unit:
        test_files = ['unit/test_worker_coordination_unit.py']
    else:
        test_files = None  # Run all

    verbose = not args.quiet
    success = run_tests(test_files, verbose, args.coverage)

    if success:
        print("\n✅ All worker coordination tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some worker coordination tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
