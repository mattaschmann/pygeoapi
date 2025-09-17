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

from copy import deepcopy
import functools
import logging
import asyncio

from geoalchemy2.functions import ST_MakeEnvelope, ST_Intersects
from sqlalchemy import select, func
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from pygeoapi.provider.base import ProviderItemNotFoundError
from pygeoapi.provider.sql import PostgreSQLProvider, get_table_model
from pygeoapi.util import get_crs_from_uri

LOGGER = logging.getLogger(__name__)


@functools.cache
def get_async_engine(
    driver_name: str,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
    **connect_args
):
    """Create async SQL Alchemy engine."""
    conn_str = URL.create(
        drivername=driver_name,
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=database
    )
    engine = create_async_engine(
        conn_str,
        connect_args=connect_args,
        pool_pre_ping=connect_args.get('pool_pre_ping', True),
        pool_size=connect_args.get('pool_size', 20),
        max_overflow=connect_args.get('max_overflow', 30),
        pool_recycle=connect_args.get('pool_recycle', 3600),
        echo=False  # Set to True for debugging SQL queries
    )
    return engine


class AsyncPostgreSQLProvider(PostgreSQLProvider):
    """
    An async provider for querying a PostgreSQL database
    """
    default_port = 5432

    def __init__(self, provider_def: dict):
        """
        AsyncPostgreSQLProvider Class constructor

        :param provider_def: provider definitions from yml pygeoapi-config.
                             data,id_field, name set in parent class
                             data contains the connection information
                             for class DatabaseCursor
        :returns: pygeoapi.provider.async_sql.AsyncPostgreSQLProvider
        """

        driver_name = 'postgresql+psycopg2'
        async_driver_name = 'postgresql+psycopg'
        extra_conn_args = {
            'client_encoding': 'utf8',
            'application_name': 'pygeoapi'
        }

        # psycopg3 specific optimizations for concurrent operations
        psycopg3_conn_args = {
            'pool_size': 20,  # Size of the connection pool
            'max_overflow': 30,  # Maximum overflow connections
            'pool_pre_ping': True,  # Validate connections before use
            'pool_recycle': 3600,  # Recycle connections after 1 hour
        }

        # Initialize parent class first
        super().__init__(provider_def)

        # Create async engine for async operations with psycopg3 optimizations
        self._async_engine = get_async_engine(
            async_driver_name,
            self.db_host,
            self.db_port,
            self.db_name,
            self.db_user,
            self._db_password,
            **self.db_options | extra_conn_args | psycopg3_conn_args
        )

        # Create async session factory
        self._async_session_factory = async_sessionmaker(
            self._async_engine, class_=AsyncSession, expire_on_commit=False
        )


    async def query_async(
        self,
        offset=0,
        limit=10,
        resulttype='results',
        bbox=[],
        datetime_=None,
        properties=[],
        sortby=[],
        select_properties=[],
        skip_geometry=False,
        q=None,
        filterq=None,
        crs_transform_spec=None,
        **kwargs
    ):
        """
        Async query sql database for all the content.

        :param offset: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)
        :param q: full-text search term(s)
        :param filterq: CQL query as text string
        :param crs_transform_spec: `CrsTransformSpec` instance, optional

        :returns: GeoJSON FeatureCollection
        """

        LOGGER.debug('Preparing filters for async query')
        property_filters = self._get_property_filters(properties)
        cql_filters = self._get_cql_filters(filterq)
        bbox_filter = self._get_bbox_filter(bbox)
        time_filter = self._get_datetime_filter(datetime_)
        order_by_clauses = self._get_order_by_clauses(sortby, self.table_model)
        selected_properties = self._select_properties_clause(
            select_properties, skip_geometry
        )

        LOGGER.debug('Querying Database asynchronously with concurrent operations')
        # Execute query within async database Session context
        async with self._async_session_factory() as session:
            # Build the select query
            stmt = (
                select(self.table_model)
                .filter(property_filters)
                .filter(cql_filters)
                .filter(bbox_filter)
                .filter(time_filter)
                .options(selected_properties)
            )

            # Get count for pagination
            count_stmt = (
                select(func.count())
                .select_from(self.table_model)
                .filter(property_filters)
                .filter(cql_filters)
                .filter(bbox_filter)
                .filter(time_filter)
            )

            # Execute count query to determine if we need data query
            count_result = await session.execute(count_stmt)
            matched = count_result.scalar()

            LOGGER.debug(f'Found {matched} result(s)')

            LOGGER.debug('Preparing response')
            response = {
                'type': 'FeatureCollection',
                'features': [],
                'numberMatched': matched,
                'numberReturned': 0
            }

            if resulttype == 'hits':
                return response

            crs_transform_out = self._get_crs_transform(crs_transform_spec)

            # Apply ordering, offset, and limit
            if order_by_clauses:
                stmt = stmt.order_by(*order_by_clauses)
            stmt = stmt.offset(offset).limit(limit)

            # Execute the main query
            result = await session.execute(stmt)
            items = result.scalars().all()

            # Process features concurrently for better performance with large result sets
            async def process_feature(item):
                return self._sqlalchemy_to_feature(item, crs_transform_out,
                                                  select_properties)

            # For small result sets, process sequentially to avoid overhead
            if len(items) <= 10:
                for item in items:
                    response['numberReturned'] += 1
                    response['features'].append(
                        self._sqlalchemy_to_feature(item, crs_transform_out,
                                                    select_properties)
                    )
            else:
                # For larger result sets, process features concurrently
                tasks = [process_feature(item) for item in items]
                features = await asyncio.gather(*tasks)
                response['features'].extend(features)
                response['numberReturned'] = len(features)

        return response

    async def get_async(self, identifier, crs_transform_spec=None, **kwargs):
        """
        Async query the provider for a specific feature id

        :param identifier: feature id
        :param crs_transform_spec: `CrsTransformSpec` instance, optional

        :returns: GeoJSON FeatureCollection
        """
        LOGGER.debug(f'Get item by ID asynchronously: {identifier}')

        # Execute query within async database Session context
        async with self._async_session_factory() as session:
            # Retrieve data from database as feature
            item = await session.get(self.table_model, identifier)
            if item is None:
                msg = f'No such item: {self.id_field}={identifier}.'
                raise ProviderItemNotFoundError(msg)

            crs_transform_out = self._get_crs_transform(crs_transform_spec)
            feature = self._sqlalchemy_to_feature(item, crs_transform_out)

            # Drop non-defined properties
            if self.properties:
                props = feature['properties']
                dropping_keys = deepcopy(props).keys()
                for item_key in dropping_keys:
                    if item_key not in self.properties:
                        props.pop(item_key)

            # Add fields for previous and next items using concurrent queries
            id_field = getattr(self.table_model, self.id_field)

            prev_stmt = (
                select(self.table_model)
                .where(id_field < identifier)
                .order_by(id_field.desc())
                .limit(1)
            )
            next_stmt = (
                select(self.table_model)
                .where(id_field > identifier)
                .order_by(id_field.asc())
                .limit(1)
            )

            # Execute prev and next queries concurrently for better performance
            prev_task = session.execute(prev_stmt)
            next_task = session.execute(next_stmt)

            prev_result, next_result = await asyncio.gather(prev_task, next_task)
            prev_item = prev_result.scalar_one_or_none()
            next_item = next_result.scalar_one_or_none()

            feature['prev'] = (
                getattr(prev_item, self.id_field)
                if prev_item is not None
                else identifier
            )
            feature['next'] = (
                getattr(next_item, self.id_field)
                if next_item is not None
                else identifier
            )

        return feature

    async def create_async(self, item):
        """
        Async create a new item

        :param item: `dict` of new item

        :returns: identifier of created item
        """

        identifier, json_data = self._load_and_prepare_item(
            item, accept_missing_identifier=True
        )

        new_instance = self._feature_to_sqlalchemy(json_data, identifier)
        async with self._async_session_factory() as session:
            session.add(new_instance)
            await session.commit()
            await session.refresh(new_instance)
            result_id = getattr(new_instance, self.id_field)

        # NOTE: need to use id from instance in case it's generated
        return result_id

    async def update_async(self, identifier, item):
        """
        Async update an existing item

        :param identifier: feature id
        :param item: `dict` of partial or full item

        :returns: `bool` of update result
        """

        identifier, json_data = self._load_and_prepare_item(
            item, raise_if_exists=False
        )

        new_instance = self._feature_to_sqlalchemy(json_data, identifier)
        async with self._async_session_factory() as session:
            await session.merge(new_instance)
            await session.commit()

        return True

    async def delete_async(self, identifier):
        """
        Async delete an existing item

        :param identifier: item id

        :returns: `bool` of deletion result
        """
        async with self._async_session_factory() as session:
            from sqlalchemy import delete as sql_delete
            id_column = getattr(self.table_model, self.id_field)
            stmt = sql_delete(self.table_model).where(id_column == identifier)
            result = await session.execute(stmt)
            await session.commit()

        return result.rowcount > 0

    def query(self, *args, **kwargs):
        """
        Sync wrapper for async query operation
        """
        return asyncio.run(self.query_async(*args, **kwargs))

    def get(self, identifier, crs_transform_spec=None, **kwargs):
        """
        Sync wrapper for async get operation
        """
        return asyncio.run(self.get_async(identifier, crs_transform_spec, **kwargs))

    def create(self, item):
        """
        Sync wrapper for async create operation
        """
        return asyncio.run(self.create_async(item))

    def update(self, identifier, item):
        """
        Sync wrapper for async update operation
        """
        return asyncio.run(self.update_async(identifier, item))

    def delete(self, identifier):
        """
        Sync wrapper for async delete operation
        """
        return asyncio.run(self.delete_async(identifier))
