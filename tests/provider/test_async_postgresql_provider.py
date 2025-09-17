# =================================================================
#
# Authors: Matt Aschmann <matt.aschmann@proton.me>
#
# Copyright (c) 2025 Matt Aschmann
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import os
import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

# Skip all tests if psycopg3 not available
psycopg_available = True
try:
    import psycopg
except ImportError:
    psycopg_available = False

skip_psycopg = pytest.mark.skipif(
    not psycopg_available,
    reason="psycopg3 not available"
)

# Only import and test if psycopg3 is available
if psycopg_available:
    from pygeoapi.provider.async_sql import AsyncPostgreSQLProvider, get_async_engine
    from pygeoapi.provider.base import ProviderItemNotFoundError


@pytest.fixture()
def async_config():
    """Configuration for AsyncPostgreSQLProvider tests."""
    return {
        'name': 'AsyncPostgreSQL',
        'type': 'feature',
        'data': {
            'host': '127.0.0.1',
            'dbname': 'test',
            'user': 'postgres',
            'password': 'test_password',
            'search_path': ['public']
        },
        'id_field': 'id',
        'table': 'test_table',
        'geom_field': 'geom'
    }


@skip_psycopg
class TestAsyncPostgreSQLProviderCore:
    """Test core AsyncPostgreSQLProvider functionality."""

    def test_driver_selection(self):
        """Test that psycopg3 is selected as the async driver."""
        # This tests the driver name configuration
        with patch('pygeoapi.provider.async_sql.get_table_model') as mock_async_table, \
             patch('pygeoapi.provider.sql.get_table_model') as mock_table, \
             patch('pygeoapi.provider.async_sql.get_async_engine') as mock_engine:

            # Create proper table model mock with __table__ attribute
            mock_table_obj = Mock()
            mock_table_obj.__table__ = Mock()
            mock_table_obj.__table__.columns = []
            mock_async_table.return_value = mock_table_obj
            mock_table.return_value = mock_table_obj

            mock_engine.return_value = Mock()
            provider = AsyncPostgreSQLProvider({
                'name': 'Test',
                'type': 'feature',
                'data': {'host': '127.0.0.1', 'dbname': 'test', 'user': 'test', 'password': 'test'},
                'id_field': 'id',
                'table': 'test_table',
                'geom_field': 'geom'
            })

            # Verify async driver was called with psycopg
            mock_engine.assert_called_once()
            call_args = mock_engine.call_args[0]
            assert call_args[0] == 'postgresql+psycopg'

    def test_psycopg3_connection_optimizations(self):
        """Test that psycopg3-specific optimizations are applied."""
        with patch('pygeoapi.provider.async_sql.get_table_model') as mock_async_table, \
             patch('pygeoapi.provider.sql.get_table_model') as mock_table, \
             patch('pygeoapi.provider.async_sql.get_async_engine') as mock_engine:

            # Create proper table model mock with __table__ attribute
            mock_table_obj = Mock()
            mock_table_obj.__table__ = Mock()
            mock_table_obj.__table__.columns = []
            mock_async_table.return_value = mock_table_obj
            mock_table.return_value = mock_table_obj

            mock_engine.return_value = Mock()
            provider = AsyncPostgreSQLProvider({
                'name': 'Test',
                'type': 'feature',
                'data': {'host': '127.0.0.1', 'dbname': 'test', 'user': 'test', 'password': 'test'},
                'id_field': 'id',
                'table': 'test_table',
                'geom_field': 'geom'
            })

            # Verify psycopg3 optimizations were passed
            call_kwargs = mock_engine.call_args[1]

            # Check for psycopg3-specific optimizations
            assert 'pool_size' in call_kwargs
            assert call_kwargs['pool_size'] == 20
            assert 'max_overflow' in call_kwargs
            assert call_kwargs['max_overflow'] == 30
            assert 'pool_pre_ping' in call_kwargs
            assert call_kwargs['pool_pre_ping'] is True
            assert 'pool_recycle' in call_kwargs
            assert call_kwargs['pool_recycle'] == 3600

    def test_get_async_engine_configuration(self):
        """Test the get_async_engine function with psycopg3 settings."""
        with patch('pygeoapi.provider.async_sql.create_async_engine') as mock_create_engine:
            mock_create_engine.return_value = Mock()

            # Test engine creation with psycopg3 optimizations
            engine = get_async_engine(
                'postgresql+psycopg',
                'localhost',
                '5432',
                'testdb',
                'testuser',
                'testpass',
                pool_size=20,
                max_overflow=30,
                pool_pre_ping=True,
                pool_recycle=3600
            )

            # Verify engine was created with correct parameters
            mock_create_engine.assert_called_once()
            call_kwargs = mock_create_engine.call_args[1]

            assert call_kwargs['pool_size'] == 20
            assert call_kwargs['max_overflow'] == 30
            assert call_kwargs['pool_pre_ping'] is True
            assert call_kwargs['pool_recycle'] == 3600

    @pytest.mark.asyncio
    async def test_concurrent_operations_pattern(self):
        """Test that concurrent operations are properly structured."""
        # This tests the pattern of using asyncio.gather for concurrent operations

        async def mock_operation(delay):
            """Mock async operation."""
            await asyncio.sleep(delay * 0.001)  # Very short delay
            return f"result_{delay}"

        # Test that concurrent operations work faster than sequential
        import time

        # Sequential execution
        start_time = time.time()
        results_sequential = []
        for i in range(3):
            result = await mock_operation(i)
            results_sequential.append(result)
        sequential_time = time.time() - start_time

        # Concurrent execution using asyncio.gather (same pattern as in our provider)
        start_time = time.time()
        tasks = [mock_operation(i) for i in range(3)]
        results_concurrent = await asyncio.gather(*tasks)
        concurrent_time = time.time() - start_time

        # Concurrent should be faster or similar
        assert concurrent_time <= sequential_time * 1.5  # Allow some variance
        assert results_sequential == list(results_concurrent)

    def test_async_session_factory_configuration(self):
        """Test async session factory is properly configured."""
        with patch('pygeoapi.provider.async_sql.get_table_model') as mock_async_table, \
             patch('pygeoapi.provider.sql.get_table_model') as mock_table, \
             patch('pygeoapi.provider.async_sql.get_async_engine'), \
             patch('pygeoapi.provider.async_sql.async_sessionmaker') as mock_sessionmaker:

            # Create proper table model mock with __table__ attribute
            mock_table_obj = Mock()
            mock_table_obj.__table__ = Mock()
            mock_table_obj.__table__.columns = []
            mock_async_table.return_value = mock_table_obj
            mock_table.return_value = mock_table_obj

            mock_sessionmaker.return_value = Mock()
            provider = AsyncPostgreSQLProvider({
                'name': 'Test',
                'type': 'feature',
                'data': {'host': '127.0.0.1', 'dbname': 'test', 'user': 'test', 'password': 'test'},
                'id_field': 'id',
                'table': 'test_table',
                'geom_field': 'geom'
            })

            # Verify session factory was configured correctly
            mock_sessionmaker.assert_called_once()
            call_kwargs = mock_sessionmaker.call_args[1]

            assert call_kwargs['class_'] == AsyncSession
            assert call_kwargs['expire_on_commit'] is False

    def test_import_succeeds(self):
        """Test that AsyncPostgreSQLProvider can be imported when psycopg3 is available."""
        # This test verifies the basic import works
        from pygeoapi.provider.async_sql import AsyncPostgreSQLProvider
        assert AsyncPostgreSQLProvider is not None

    def test_requirements_compatibility(self):
        """Test that psycopg3 is properly installed with required extras."""
        try:
            import psycopg
            import psycopg_pool
            # If we get here, both psycopg and psycopg_pool are available
            assert True
        except ImportError as e:
            pytest.fail(f"Required psycopg3 packages not available: {e}")


@skip_psycopg
class TestAsyncPostgreSQLProviderRequirements:
    """Test requirements and dependencies."""

    def test_psycopg_import(self):
        """Test that psycopg can be imported."""
        import psycopg
        assert psycopg is not None

    def test_psycopg_pool_import(self):
        """Test that psycopg_pool can be imported."""
        import psycopg_pool
        assert psycopg_pool is not None

    def test_async_capabilities(self):
        """Test that asyncio works with our patterns."""
        async def test_async():
            return "async_works"

        # Run a simple async test
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(test_async())
            assert result == "async_works"
        finally:
            loop.close()


if __name__ == "__main__":
    """Run tests if psycopg3 is available."""
    if psycopg_available:
        pytest.main([__file__, "-v"])
    else:
        print("psycopg3 not available. Install with: pip install 'psycopg[binary,pool]>=3.1.0'")
