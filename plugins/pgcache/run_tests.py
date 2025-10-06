#!/usr/bin/env python3
"""
Comprehensive test runner for pgcache module.

This script runs all test suites and provides detailed reporting:
1. Unit tests (test_pgcache.py)
2. Integration tests (test_integration.py)
3. Performance benchmarks
4. Test coverage reporting
"""

import subprocess
import sys
import os
import json
import time
import argparse
from pathlib import Path
from typing import Dict, List, Any

class TestRunner:
    """Comprehensive test runner for pgcache"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.results = {
            'unit_tests': [],
            'integration_tests': [],
            'performance_tests': [],
            'summary': {
                'total_tests': 0,
                'passed': 0,
                'failed': 0,
                'errors': 0,
                'start_time': None,
                'end_time': None
            }
        }

    def log(self, message: str, level: str = "INFO"):
        """Log message with timestamp"""
        timestamp = time.strftime("%H:%M:%S")
        if self.verbose or level == "ERROR":
            print(f"[{timestamp}] {level}: {message}")

    def run_command(self, cmd: List[str], timeout: int = 300) -> Dict[str, Any]:
        """Run a command and return results"""
        try:
            self.log(f"Running: {' '.join(cmd)}", "DEBUG")

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )

            return {
                'success': result.returncode == 0,
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr
            }

        except subprocess.TimeoutExpired:
            self.log(f"Command timed out: {' '.join(cmd)}", "ERROR")
            return {
                'success': False,
                'returncode': -1,
                'stdout': '',
                'stderr': 'Timeout'
            }
        except Exception as e:
            self.log(f"Command failed: {' '.join(cmd)} - {e}", "ERROR")
            return {
                'success': False,
                'returncode': -2,
                'stdout': '',
                'stderr': str(e)
            }

    def run_unit_tests(self) -> bool:
        """Run unit tests"""
        self.log("Running unit tests...")

        result = self.run_command(['python3', 'test_pgcache.py'])

        if result['success']:
            self.log("✅ Unit tests completed successfully")
            self.results['unit_tests'].append({
                'status': 'PASS',
                'output': result['stdout']
            })
            return True
        else:
            self.log("❌ Unit tests failed")
            self.results['unit_tests'].append({
                'status': 'FAIL',
                'output': result['stderr']
            })
            return False

    def run_integration_tests(self) -> bool:
        """Run integration tests"""
        self.log("Running integration tests...")

        result = self.run_command(['python3', 'test_integration.py'])

        if result['success']:
            self.log("✅ Integration tests completed successfully")
            self.results['integration_tests'].append({
                'status': 'PASS',
                'output': result['stdout']
            })
            return True
        else:
            self.log("❌ Integration tests failed")
            self.results['integration_tests'].append({
                'status': 'FAIL',
                'output': result['stderr']
            })
            return False

    def run_performance_tests(self) -> bool:
        """Run performance benchmarks"""
        self.log("Running performance tests...")

        # Basic performance test - measure cache operations
        perf_script = """
import redis
import time
import json

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Test cache performance
start_time = time.time()

# Perform 100 cache operations
for i in range(100):
    key = f"perf_test_{i}"
    value = f"value_{i}"
    r.setex(key, 300, value)
    r.get(key)

end_time = time.time()
total_time = end_time - start_time

print(json.dumps({
    'operations_per_second': 100 / total_time,
    'total_time': total_time,
    'operations': 100
}))
"""

        result = self.run_command(['python3', '-c', perf_script])

        if result['success']:
            try:
                perf_data = json.loads(result['stdout'].strip())
                self.log(f"✅ Performance test: {perf_data['operations_per_second']:.2f} ops/sec")
                self.results['performance_tests'].append({
                    'status': 'PASS',
                    'data': perf_data
                })
                return True
            except:
                self.log("❌ Failed to parse performance data", "ERROR")
                return False
        else:
            self.log("❌ Performance test failed", "ERROR")
            return False

    def generate_report(self) -> str:
        """Generate comprehensive test report"""
        report = []
        report.append("=" * 60)
        report.append("PGCACHE TEST REPORT")
        report.append("=" * 60)

        # Summary
        summary = self.results['summary']
        duration = "N/A"
        if summary['start_time'] and summary['end_time']:
            duration = f"{summary['end_time'] - summary['start_time']:.2f}s"

        report.append("
📊 SUMMARY:"        report.append(f"   Total Tests: {summary['total_tests']}")
        report.append(f"   Passed: {summary['passed']}")
        report.append(f"   Failed: {summary['failed']}")
        report.append(f"   Errors: {summary['errors']}")
        report.append(f"   Duration: {duration}")

        # Unit Tests
        if self.results['unit_tests']:
            report.append("
🧪 UNIT TESTS:"            for test in self.results['unit_tests']:
                status = "✅ PASS" if test['status'] == 'PASS' else "❌ FAIL"
                report.append(f"   {status}")

        # Integration Tests
        if self.results['integration_tests']:
            report.append("
🔗 INTEGRATION TESTS:"            for test in self.results['integration_tests']:
                status = "✅ PASS" if test['status'] == 'PASS' else "❌ FAIL"
                report.append(f"   {status}")

        # Performance Tests
        if self.results['performance_tests']:
            report.append("
⚡ PERFORMANCE TESTS:"            for test in self.results['performance_tests']:
                if test['status'] == 'PASS':
                    data = test['data']
                    report.append(f"   ✅ {data['operations_per_second']:.2f} ops/sec ({data['total_time']:.3f}s for {data['operations']} ops)")
                else:
                    report.append("   ❌ FAILED")

        report.append("\n" + "=" * 60)

        return "\n".join(report)

    def save_results(self):
        """Save detailed results to file"""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"test_results_{timestamp}.json"

        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)

        self.log(f"📄 Detailed results saved to: {filename}")

    def run_all_tests(self) -> bool:
        """Run complete test suite"""
        print("🚀 Starting comprehensive pgcache test suite...")

        self.results['summary']['start_time'] = time.time()

        # Check if pgcache module is available
        result = self.run_command(['redis-cli', 'COMMAND', 'INFO', 'PGCACHE.STATUS'])
        if not result['success']:
            print("❌ pgcache module not available. Please ensure it's loaded.")
            return False

        # Run test suites
        tests_passed = 0
        total_tests = 0

        # Unit tests
        total_tests += 1
        if self.run_unit_tests():
            tests_passed += 1

        # Integration tests
        total_tests += 1
        if self.run_integration_tests():
            tests_passed += 1

        # Performance tests
        total_tests += 1
        if self.run_performance_tests():
            tests_passed += 1

        self.results['summary']['end_time'] = time.time()
        self.results['summary']['total_tests'] = total_tests
        self.results['summary']['passed'] = tests_passed
        self.results['summary']['failed'] = total_tests - tests_passed

        # Generate and display report
        report = self.generate_report()
        print(report)

        # Save detailed results
        self.save_results()

        success = tests_passed == total_tests
        if success:
            print("\n🎉 All tests passed! pgcache is working correctly.")
        else:
            print(f"\n❌ {total_tests - tests_passed} test(s) failed.")

        return success

def main():
    """Main test runner"""
    parser = argparse.ArgumentParser(description='Run pgcache tests')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--setup', action='store_true', help='Run setup before tests')
    parser.add_argument('--unit-only', action='store_true', help='Run only unit tests')
    parser.add_argument('--integration-only', action='store_true', help='Run only integration tests')

    args = parser.parse_args()

    if args.setup:
        print("🔧 Setting up test environment...")
        result = subprocess.run(['python3', 'setup_test_environment.py'])
        if result.returncode != 0:
            print("❌ Setup failed")
            sys.exit(1)

    runner = TestRunner(verbose=args.verbose)

    try:
        if args.unit_only:
            success = runner.run_unit_tests()
        elif args.integration_only:
            success = runner.run_integration_tests()
        else:
            success = runner.run_all_tests()

        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        print("\n⚠️ Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Test runner failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
