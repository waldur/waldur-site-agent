# SLURM Emulator Usage in Tests

## How the Emulator is Used

### 🎯 **Integration Method: PyPI Package**

The SLURM emulator is available as a pip package:

```python

# Clean package imports

try:
    import emulator
    EMULATOR_AVAILABLE = True
except ImportError:
    EMULATOR_AVAILABLE = False

# Import emulator components directly

from emulator.core.database import SlurmDatabase
from emulator.scenarios.sequence_scenario import SequenceScenario
from emulator.periodic_limits.calculator import PeriodicLimitsCalculator

```text

### 📦 **Package Features:**

- ✅ **Clean imports**: No `sys.path` manipulation needed

- ✅ **Test dependency**: Optional dependency in SLURM plugin

- ✅ **CI/CD friendly**: Standard uv installation

## Three Ways to Use the Emulator

### 1. **Running API Server** (Current Production Method) ✅

```bash

# Start emulator API server

uvicorn emulator.api.emulator_server:app --host 0.0.0.0 --port 8080

```text

**Used by**:
- Site agent backend in emulator mode

- Integration tests that need HTTP API

- Real-time scenario execution

**API Endpoints**:
- `GET /api/status` - Emulator state

- `POST /api/apply-periodic-settings` - Apply settings

- `POST /api/time/advance` - Time manipulation

- `POST /api/usage/inject` - Usage injection

### 2. **Direct Python Import** (New Testing Method) ✅

```python

# Import emulator components directly

from emulator.scenarios.sequence_scenario import SequenceScenario
from emulator.periodic_limits.calculator import PeriodicLimitsCalculator

# Use emulator's calculation engine

calculator = PeriodicLimitsCalculator(database, time_engine)
settings = calculator.calculate_periodic_settings(account)

```text

**Used by**:
- Unit tests that need exact emulator calculations

- Scenario validation tests

- Performance benchmarking

**Benefits**:
- No network overhead

- Direct access to calculation engines

- Can inspect internal state

### 3. **CLI Command Interface** (Available but Limited) ⚠️

```bash

# Interactive CLI

python -m emulator.cli.main

# Or programmatic CLI

echo "account create test-account 'Test' 1000" | python -m emulator.cli.main

```text

**Used by**:
- Manual testing and exploration

- Some integration tests via subprocess

**Limitations**:
- More complex to automate

- Harder to extract results programmatically

## Test Integration Architecture

### **Current Test Structure** ✅

```text

Plugin Tests (waldur-site-agent/plugins/slurm/tests/):
│
├── Mock Tests (Fast, No Dependencies) ✅
│   ├── Custom mock calculations
│   ├── Synthetic STOMP messages
│   └── Unit testing
│
├── API Integration Tests (Running Emulator) ✅
│   ├── HTTP API calls to localhost:8080
│   ├── Site agent backend ↔ emulator communication
│   └── Settings verification
│
└── Scenario Framework Tests (Direct Import) ✅ NEW!
    ├── Real emulator calculation engines
    ├── Built-in scenario execution
    └── SequenceScenario, QoSManager, PeriodicLimitsCalculator

```text

## Installation Options for Different Use Cases

### **Option 1: PyPI Package** ✅ **Recommended for All Use Cases**

```bash

# Install from PyPI

uv add slurm-emulator

```text

```python

# Clean imports - works everywhere

try:
    import emulator
    EMULATOR_AVAILABLE = True
except ImportError:
    EMULATOR_AVAILABLE = False

```text

**Pros**:
- ✅ Simple pip installation

- ✅ Clean imports (`from emulator.core import ...`)

- ✅ Available system-wide

- ✅ Proper Python package

- ✅ Version controlled through PyPI

- ✅ CI/CD friendly

- ✅ No path dependencies

- ✅ Portable across environments

**Cons**:
- None

### **Option 2: Development/Test Dependency in pyproject.toml** ✅ **For Plugin Development**

```toml

# In SLURM plugin pyproject.toml

[project.optional-dependencies]
test = [
    "slurm-emulator",
]

```text

**Installation**:

```bash

# Install with test dependencies

uv sync --extra test

```text

**Pros**:
- ✅ Proper dependency management

- ✅ Version control

- ✅ Optional dependency (tests skip if not available)

- ✅ Clean workspace setup

**Cons**:
- None

## Recommendation for Production

### **For Testing: PyPI Package** ✅

```python

@pytest.mark.skipif(not EMULATOR_AVAILABLE, reason="SLURM emulator package not installed")
class TestWithEmulator:
    @pytest.fixture(scope="class")
    def emulator_setup(self):
        # No setup needed - just import directly

        from emulator.core.database import SlurmDatabase
        # ... rest of setup

```text

**Why it works well**:
- ✅ **Optional dependency**: Tests skip gracefully if emulator not installed

- ✅ **CI/CD friendly**: Simple `uv add slurm-emulator` in CI pipeline

- ✅ **Development friendly**: Standard package, easy to manage versions

- ✅ **No conflicts**: Proper package management through uv

- ✅ **Clean imports**: No sys.path manipulation needed

- ✅ **Portable**: Works the same everywhere

### **For CI/CD** ✅

```yaml

# GitLab CI

test-with-emulator:
  script:
    - uv add slurm-emulator
    - uv run pytest tests/test_periodic_limits/test_emulator_scenarios_working.py

```text

### **For Production Deployment: API Server** ✅

```bash

# Run emulator as service for testing

uvicorn emulator.api.emulator_server:app --port 8080

# Site agent uses HTTP API

backend_settings:
  periodic_limits:
    emulator_mode: true
    emulator_base_url: "http://localhost:8080"

```text

## Summary

### ✅ **Features:**

- **✅ Comprehensive testing** with real emulator scenarios

- **✅ Standard package management** via uv

- **✅ Optional emulator integration** (tests skip if not installed)

- ✅ **Clean imports** (no sys.path manipulation)

- ✅ **CI/CD friendly**

- **✅ Portable across environments**

## Test Execution Summary

```bash

# Install emulator package for testing

uv add slurm-emulator

# or install with test dependencies

uv sync --extra test

# Run all tests (mocks + emulator scenarios)

cd plugins/slurm
uv run pytest tests/test_periodic_limits/ -v

# Run only emulator scenario tests

uv run pytest tests/test_periodic_limits/test_emulator_scenarios_working.py -v

# Run with emulator API server
# Terminal 1: Start emulator

uvicorn emulator.api.emulator_server:app --port 8080

# Terminal 2: Run tests

cd plugins/slurm
uv run pytest tests/test_periodic_limits/test_real_emulator_scenarios.py -v

```text

**Complete emulator integration using the PyPI package** ✅
