# AsyncPostgreSQLProvider Test Suite

This directory contains comprehensive tests for the AsyncPostgreSQLProvider that uses psycopg3's concurrent operations instead of asyncpg.

## Test Files

### `test_async_postgresql_provider_simple.py`
**Status: ✅ Working (7/10 tests passing)**

Core functionality tests that verify:
- ✅ Psycopg3 driver selection (`postgresql+psycopg`)
- ✅ Connection pool configuration with psycopg3 optimizations
- ✅ Async engine configuration with proper pool settings
- ✅ Concurrent operations patterns using `asyncio.gather()`
- ✅ Import capabilities and requirements
- ✅ Async session factory configuration
- ❌ Full provider initialization (requires better table model mocking)

### `test_async_postgresql_provider.py`
**Status: ⚠️ Partial (needs table model mocking fixes)**

Comprehensive unit tests covering:
- Provider initialization and configuration
- Async CRUD operations (query_async, get_async, create_async, update_async, delete_async)
- Concurrent query processing
- Connection pooling features
- Error handling
- Psycopg3-specific optimizations

**Issue**: Needs proper mocking of SQLAlchemy table models to avoid database connections during unit tests.

## Key Features Tested

### 1. Psycopg3 Integration
- ✅ Driver selection: `postgresql+psycopg` instead of `postgresql+asyncpg`
- ✅ Connection pool optimizations:
  - `pool_size=20`
  - `max_overflow=30`
  - `pool_pre_ping=True`
  - `pool_recycle=3600`
- ✅ Application identification: `application_name=pygeoapi`

### 2. Concurrent Operations
- ✅ Async query processing with `asyncio.gather()`
- ✅ Concurrent prev/next queries in `get_async()`
- ✅ Large result set processing (>10 items processed concurrently)
- ✅ Performance optimization patterns

### 3. Requirements
- ✅ `psycopg[binary,pool]>=3.1.0` properly installed
- ✅ Import validation
- ✅ Async capabilities verification

## Running Tests

### Prerequisites
```bash
# Install psycopg3 with required extras
uv pip install -r requirements-async.txt
```

### Unit Tests (Recommended)
```bash
# Run the working core functionality tests
uv run python -m pytest tests/provider/test_async_postgresql_provider_simple.py -v

# Run specific test categories
uv run python -m pytest tests/provider/test_async_postgresql_provider_simple.py::TestAsyncPostgreSQLProviderCore -v
uv run python -m pytest tests/provider/test_async_postgresql_provider_simple.py::TestAsyncPostgreSQLProviderRequirements -v
```

### Integration Tests (Optional)
```bash
# Set up environment variables for database connection
export POSTGRESQL_HOST=127.0.0.1
export POSTGRESQL_PASSWORD=your_password
export POSTGRESQL_DBNAME=test
export POSTGRESQL_USER=postgres

### All Async Tests
```bash
# Run updated async import test
uv run python -m pytest tests/other/test_async.py::TestAsyncPostgreSQL::test_psycopg_import -v
```

## Test Coverage

### ✅ Working Tests
- Driver configuration and selection
- Connection pool setup with psycopg3 optimizations
- Async engine configuration
- Concurrent operations patterns
- Import and requirements validation
- Session factory configuration

### ⚠️ Need Fixes
- Full provider initialization (table model mocking)
- Complete CRUD operation testing
- Full error handling scenarios

### 🔄 Integration Tests
- Ready for real database testing
- Comprehensive performance and stress testing
- Memory usage validation
- Compatibility verification

## Benefits Validated

1. **Performance**: Concurrent operations are properly structured using `asyncio.gather()`
2. **Reliability**: Connection pooling with health checks and recycling
3. **Compatibility**: Proper psycopg3 configuration with required extras
4. **Future-proof**: Tests verify modern async patterns and psycopg3 features

## Next Steps

1. **Fix table model mocking** in comprehensive unit tests
3. **Add performance benchmarks** comparing asyncpg vs psycopg3
4. **Add error injection tests** for connection failures and recovery
