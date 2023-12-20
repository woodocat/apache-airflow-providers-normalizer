import yaml
import json
from contextlib import closing
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.context import Context

from typing import Union, Callable, Optional, Sequence

from .utils import MappingElements, get_table, get_fields, normalize, flatten
from .queries import (
    DROP_TABLE_QUERY,
    CREATE_TABLE_QUERY,
    INSERT_INTO_QUERY,
    SELECT_ALL_QUERY,
    SELECT_COUNT_QUERY,
    SELECT_MAX_QUERY,
)


DATABASES_MULTIPLE_VALUES_INSERT_SUPPORTS = (
    'clickhouse',
    'greenplum',
    'mssql',
    'mysql',
    'postgres',
    'presto',
    'sqlite',
    'trino',
)


class NormalizerOperator(BaseOperator):
    """
    This operator allows you to read json data from one database and save it
    as nested tables to another database using mapping with YAML specification.

    :param source_conn_id: reference to source database that contains tables
        with text/json/jsonb data to be processed.
    :param destination_conn_id: reference to destination database where is the
        nested tables are placed.
    :param mapping: YAML specification. Can receive a str representing a
        yaml, or reference to a template file. Template reference recognized
        by str ending with '.yaml'.
        (templated)
    :param select_count_query: (optional) the SQL code to calculate total in
        pagination as a single string, or a reference to a template file.
        (templated)
    :param select_all_query: (optional) the SQL code to select rows used in
        pagination as a single string, or a reference to a template file.
        (templated)
    :param create_table_query: (optional) SQL code template used for creating
        nested tables as a single string, or a reference to a template file.
        (templated)
    :param incremental: if True each next run appending delta rows to nested
        tables and continue id sequence, if False, each run starts full-refresh
        with id started with 1.
        (default value: False)
    """

    template_fields: Sequence[str] = (
        "mapping",
        "drop_table_query",
        "create_table_query",
        "insert_into_query",
        "select_all_query",
        "select_count_query",
    )

    template_fields_renderers = {
        "mapping": "jinja",
        "drop_table_query": "sql",
        "create_table_query": "sql",
        "insert_into_query": "sql",
        "select_all_query": "sql",
        "select_count_query": "sql",
    }

    template_ext: Sequence[str] = (".yaml", '.sql', '.hql', )

    ui_color = '#e7ffda'

    def __init__(
        self,
        *,
        source_conn_id: str,
        destination_conn_id: str,
        mapping: Union[str, dict],
        incremental: Optional[str] = None,
        preprocessing: Optional[Callable] = None,
        justify_varchar: Optional[Callable] = None,
        primary_key_name: Optional[str] = None,
        primary_key_type: Optional[str] = None,
        primary_key_short: Optional[bool] = None,
        primary_key_delim: Optional[bool] = None,
        drop_table_query: Optional[str] = None,
        create_table_query: Optional[str] = None,
        insert_into_query: Optional[str] = None,
        select_all_query: Optional[str] = None,
        select_count_query: Optional[str] = None,
        select_max_query: Optional[str] = None,
        commit_every: Optional[int] = None,
        insert_args: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_conn_id = source_conn_id
        self.destination_conn_id = destination_conn_id
        self.mapping = mapping
        self.incremental = incremental or False
        self.preprocessing = preprocessing
        self.justify_varchar = justify_varchar or True
        self.primary_key_name = primary_key_name or "id"
        self.primary_key_type = primary_key_type or "bigint"
        self.primary_key_short = primary_key_short or True
        self.primary_key_delim = primary_key_delim or "__"
        self.drop_table_query = drop_table_query or DROP_TABLE_QUERY
        self.create_table_query = create_table_query or CREATE_TABLE_QUERY
        self.insert_into_query = insert_into_query or INSERT_INTO_QUERY
        self.select_all_query = select_all_query or SELECT_ALL_QUERY
        self.select_count_query = select_count_query or SELECT_COUNT_QUERY
        self.select_max_query = select_max_query or SELECT_MAX_QUERY
        self.commit_every = commit_every or 1000
        self.insert_args = insert_args or {}

    def execute(self, context: Context):
        """Prepare and start normalization for every root table."""

        self.source_hook = BaseHook.get_hook(self.source_conn_id)
        self.destination_hook = BaseHook.get_hook(self.destination_conn_id)

        mapping_dict = yaml.load(self.mapping, Loader=yaml.Loader)

        deepness = [key.count(".") for key in mapping_dict.keys()]
        roots = [get_table(key) for key in mapping_dict.keys() if key.count(".") == min(deepness)]
        for root in roots:
            filtered = {key: value for key, value in mapping_dict.items() if root in key}
            mappings = {get_table(key): MappingElements(mapping_dict[key]) for key in filtered}
            selected = [get_fields(key) for key in filtered.keys() if get_table(key) == root][0]

            # Header-style
            if selected:
                fields = [x.strip() for x in selected.split("+")]
                columns = [x.strip("*") for x in fields]
                expands = [x.strip("*") for x in fields if "**" in x]

            # Body-style
            else:
                fields = list({x.split(".")[0] for x in mappings[root].original})
                columns = [x.strip("*") for x in fields]
                extract = [x for x in mappings[root].original if "**" in x or "." in x]
                expands = {x.split(".")[0]: [] for x in extract}
                for item in extract:
                    if "." in item:
                        column, field = item.split(".")
                        expands[column].append(field)

            self.log.info("Table processing: %s", root)
            self.log.info("Columns to be selected: %s", columns)
            self.log.info("Columns to be unpacked: %s", expands)

            self.initialize(root, mappings)
            self.normalize(root, mappings, columns, expands)

    def initialize(self, root: str, mappings: dict):
        """Initialize buffers and create tables."""

        self.log.info("Initialize buffers for %s", self.destination_conn_id)
        self.ids = {}
        self.buffers = {}
        self.relations = {}

        queries = []
        table = root.split(".")[-1]
        serial = self.primary_key_type
        short = self.primary_key_short
        delim = self.primary_key_delim
        for key in mappings:
            self.buffers[key] = []

            destination = mappings[key].destination

            # Get relations
            parts = key.replace(root, table).split(".")
            parent_key = ".".join(parts[:-1]).replace(table, root)
            parent = mappings[parent_key].destination if parent_key else None
            this = parts[-1]

            # By default is `id`, otherwise `<this_table>__id`
            pk = self.primary_key_name if short else f"{this}{delim}{self.primary_key_name}"
            pk_type = [f"{pk} {serial}"]

            # Only child tables contains a foreign key, by default is `parent_table__id`
            fk = f"{parent}{delim}{self.primary_key_name}" if parent else None
            fk_type = [f"{fk} {serial}"] if fk else []

            self.relations[key] = (fk, pk)
            definition = ", ".join(fk_type + pk_type + mappings[key].definition)

            parameters = {
                'pk': pk,
                'fk': fk,
                'table': destination,
                'definition': definition,
            }

            if self.incremental:
                try:
                    latest_id = self.destination_hook.get_first(
                        self.select_max_query.format(**parameters)
                    )[0]
                    self.ids[key] = latest_id or 0
                except Exception:
                    self.log.warning(f"Table `{key}` have not been created yet")
                    self.ids[key] = 0
            else:
                self.ids[key] = 0
                queries += [self.drop_table_query.format(**parameters)]

            if isinstance(self.create_table_query, str):
                self.create_table_query = [self.create_table_query]

            queries += [sql.format(**parameters) for sql in self.create_table_query]

        self.destination_hook.run(queries)

    def normalize(self, root: str, mappings: dict, columns: list, expands: list):
        """Restructuring data attributes into nested tables."""

        self.log.info("Extracting data from %s", self.source_conn_id)
        multiple = self.destination_hook.conn_type in DATABASES_MULTIPLE_VALUES_INSERT_SUPPORTS

        parameters = {
            'table': root,
            'fields': ", ".join(columns),
        }

        total = self.source_hook.get_first(
            self.select_count_query.format(**parameters)
        )[0]
        self.log.info(f"Total records found: {total}")

        limit = self.commit_every
        self.log.info(f"Partition size: {limit}")

        for offset in range(0, total, limit):
            self.log.info(f"Processing: {offset}/{total}")

            # TODO: it's different for mssql and oracle
            sql = self.select_all_query.format(**parameters) + f"""
                LIMIT {limit}
                OFFSET {offset}
            """
            rows = self.source_hook.get_records(sql)

            # Start processing chunk
            for row in rows:
                document = {}

                for index, column_name in enumerate(columns):
                    cell = row[index]

                    # Header-style: merge fields into document
                    if isinstance(expands, list):
                        # json field
                        if column_name in expands and isinstance(cell, dict):
                            document.update(cell)
                        # text field as json
                        elif column_name in expands and isinstance(cell, str):
                            document.update(json.loads(cell))
                        # generic value
                        else:
                            document[column_name] = cell

                    # Body-style: extracting fields from json
                    if isinstance(expands, dict):
                        data = None
                        # json field
                        if column_name in expands.keys() and isinstance(cell, dict):
                            data = cell
                        # text field as json
                        if f"{column_name}**" in expands.keys():
                            column_name = f"{column_name}**"
                            data = json.loads(cell) if isinstance(cell, str) else cell

                        if data is not None:
                            # whole json
                            if expands[column_name] == []:
                                document[column_name] = [data]
                            # single fields
                            else:
                                flat = flatten(data)
                                for field in expands[column_name]:
                                    document[f"{column_name}.{field}"] = flat[field] if field in flat else None
                        else:
                            document[column_name] = cell

                # One document can be preprocessed to a list of documents with user defined function
                documents = self.preprocessing(document) if callable(self.preprocessing) else [document]

                normalize(documents, self.buffers, self.ids, mappings, root, stringify_values=multiple)

            # Use multiple values inserting if supported
            if multiple:
                with closing(self.destination_hook.get_conn()) as conn:
                    with closing(conn.cursor()) as cur:

                        for key in mappings:
                            destination = mappings[key].destination

                            if len(self.buffers[key]) == 0:
                                self.log.info(f"No records found for `{key}`")
                                self.buffers[key] = []
                                continue

                            fk, pk = self.relations[key]
                            fk = [fk] if fk else []
                            pk = [pk] if pk else []
                            fields = ", ".join(fk + pk + mappings[key].fields)

                            self.log.info(f"Insert {len(self.buffers[key])} lines to `{key}`")
                            cur.execute(
                                self.insert_into_query.format(
                                    table=destination,
                                    fields=fields,
                                    values=", ".join(self.buffers[key]),
                                )
                            )

                            self.buffers[key] = []

                        conn.commit()

            # Otherwise, generates tonns of singe INSERT INTO operators
            else:
                for key in mappings:
                    destination = mappings[key].destination
                    # TODO: implementation "stringify no"
                    self.destination_hook.insert_rows(
                        table=destination,
                        rows=self.buffers[key],
                        **self.insert_args
                    )
                    self.buffers[key] = []

        del self.ids
        del self.buffers
        del self.relations
