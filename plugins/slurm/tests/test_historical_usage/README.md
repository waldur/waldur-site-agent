# SLURM Historical Usage Tests

This directory contains comprehensive tests for the SLURM historical usage functionality using the `slurm-emulator` package.

## Test Structure

### Core Test Files

- **`conftest.py`** - Pytest fixtures and configuration for historical usage tests

- **`test_slurm_client_historical.py`** - Tests for `SlurmClient.get_historical_usage_report()`

- **`test_slurm_backend_historical.py`** - Tests for `SlurmBackend.get_historical_usage_report()`

- **`test_historical_usage_loader.py`** - Tests for the historical usage loading command

- **`test_backend_utils_historical.py`** - Tests for backend utility functions

- **`test_integration.py`** - End-to-end integration tests

- **`README.md`** - This documentation file

### Test Categories

#### Unit Tests (No Emulator Required)

- Date parsing and validation

- Monthly period generation

- Error handling logic

- Data structure validation

#### Integration Tests (Requires Emulator)

- SLURM command emulation

- Historical data injection and retrieval

- Unit conversion accuracy

- Multi-month workflows

## Running Tests

### Prerequisites

Install the SLURM emulator development dependency:

```bash

# From the SLURM plugin directory

uv add --dev slurm-emulator

```text

### Test Execution

#### Run All Historical Tests

```bash

# Using the test runner script

python run_historical_tests.py

# Or directly with pytest

pytest tests/test_historical_usage/ -v -m historical

```text

#### Run Only Unit Tests (No Emulator)

```bash

python run_historical_tests.py --type unit

# Or with pytest

pytest tests/test_historical_usage/test_backend_utils_historical.py -v

```text

#### Run Only Integration Tests (Requires Emulator)

```bash

python run_historical_tests.py --type emulator

# Or with pytest

pytest tests/test_historical_usage/ -v -m "emulator and historical"

```text

#### Run Specific Test Files

```bash

# Test SLURM client functionality

pytest tests/test_historical_usage/test_slurm_client_historical.py -v

# Test backend functionality

pytest tests/test_historical_usage/test_slurm_backend_historical.py -v

# Test command functionality

pytest tests/test_historical_usage/test_historical_usage_loader.py -v

```text

## Test Data Setup

The tests use a consistent historical dataset across multiple months:

### Test Accounts

- **`test_account_123`** - Primary test account with historical usage data

### Test Users

- **`testuser1`** - User with varying usage across months

- **`testuser2`** - User with different usage patterns

### Historical Usage Data (2024)

| Month | testuser1 | testuser2 | Total |
|-------|-----------|-----------|-------|
| Jan   | 150h      | 100h      | 250h  |
| Feb   | 200h      | 150h      | 350h  |
| Mar   | 100h      | 250h      | 350h  |

### TRES Components Tested

- **CPU** - Converted from CPU-minutes to k-Hours (factor: 60000)

- **Memory** - Converted from MB-minutes to gb-Hours (factor: 61440)

- **GPU** - Converted from GPU-minutes to gpu-Hours (factor: 60)

## Test Fixtures

### Key Fixtures Available

- **`emulator_available`** - Skips tests if slurm-emulator not installed

- **`time_engine`** - SLURM emulator time manipulation engine

- **`slurm_database`** - Clean database with test accounts/users

- **`historical_usage_data`** - Pre-populated usage records across months

- **`patched_slurm_client`** - Redirects SLURM commands to emulator

- **`mock_slurm_tres`** - SLURM TRES configuration for testing

- **`mock_waldur_resources`** - Mock Waldur API resources

- **`mock_offering_users`** - Mock Waldur offering users

## Test Scenarios Covered

### Client-Level Tests

- ✅ Basic historical usage retrieval

- ✅ Multiple month queries

- ✅ Empty month handling

- ✅ Non-existent account handling

- ✅ TRES data validation

- ✅ Date filtering accuracy

- ✅ Multiple account queries

- ✅ Consistency with current usage methods

### Backend-Level Tests

- ✅ Historical usage processing

- ✅ SLURM to Waldur unit conversion

- ✅ Usage aggregation (users → total)

- ✅ Multi-month consistency

- ✅ Empty result handling

- ✅ Component filtering

- ✅ Data type validation

### Command-Level Tests

- ✅ Date range parsing and validation

- ✅ Staff user authentication

- ✅ Resource usage submission

- ✅ User usage submission

- ✅ Monthly processing workflow

- ✅ Error handling scenarios

### Integration Tests

- ✅ Full client→backend workflow

- ✅ Multi-month consistency

- ✅ Time manipulation effects

- ✅ Unit conversion accuracy

- ✅ Error resilience

- ✅ Large date range simulation

- ✅ Multiple account performance

## Troubleshooting

### Common Issues

#### SLURM Emulator Not Found

```text

❌ SLURM emulator not found. Install with: uv add --dev slurm-emulator

```text

**Solution**: Install the development dependency

#### Import Errors

```text

ModuleNotFoundError: No module named 'waldur_site_agent_slurm'

```text

**Solution**: Run tests from the plugin directory or check Python path

#### Test Skipped Messages

```text

SKIPPED [1] tests/conftest.py:XX: slurm-emulator not installed

```text

**Expected**: Unit tests will run, emulator tests will be skipped if emulator unavailable

### Debug Mode

Enable verbose logging for debugging:

```bash

pytest tests/test_historical_usage/ -v -s --tb=long

```text

Add debug prints to specific tests by modifying the test files temporarily.

## Test Coverage

The test suite provides comprehensive coverage of:

- ✅ **Historical Usage Retrieval** - All client methods and data flows

- ✅ **Unit Conversion** - SLURM to Waldur unit transformation accuracy

- ✅ **Date Handling** - Monthly period generation and date filtering

- ✅ **Error Handling** - Graceful handling of invalid inputs and edge cases

- ✅ **Integration** - End-to-end workflows using emulated SLURM commands

- ✅ **Performance** - Multi-account and multi-month processing efficiency

- ✅ **Data Integrity** - Correct aggregation and validation of usage data

## Contributing

When adding new historical usage functionality:

1. **Add Unit Tests** - Test core logic without emulator dependencies
2. **Add Integration Tests** - Test with emulator for realistic scenarios
3. **Update Fixtures** - Extend test data if needed
4. **Mark Tests Appropriately** - Use `@pytest.mark.emulator` and `@pytest.mark.historical`
5. **Update Documentation** - Add new test scenarios to this README

### Test Naming Convention

- `test_*_basic` - Simple functionality tests

- `test_*_multiple_*` - Tests with multiple inputs/iterations

- `test_*_empty_*` - Tests with no data scenarios

- `test_*_invalid_*` - Tests with invalid input handling

- `test_*_integration` - End-to-end workflow tests

- `test_*_performance` - Performance and scalability tests
