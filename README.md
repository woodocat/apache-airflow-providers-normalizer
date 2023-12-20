# Normalizer Provider for Apache Airflow

This package contains operator to normalize JSON as text or JSONB field to nested
tables by YAML specification.


## Installation

You can install provider with any package manager compatible with PyPI:

```
pip install apache-airflow-provider-normalizer
```


## Features

- Declarative specification in YAML.
- Can extract only specified fields.
- Work with json/jsonb or general fields.
- Resistant to the absence of any fields.
- Automaticaly creates all nested tables.
- Supports incremental and full-refresh mode.
- One YAML can contains multiple root tables.
- Root table is normalized with snowflake schema.


## Example

Available two specification styles.

```yaml
# Body-style mapping
postgres.public.orders:                                         # full table name or short is supported
  staging.order:                                                # table can be renamed in destination
    id*:                      { origin_id: bigint }             # use "*" to mark primary key
    dt_created:               { date_created: timestamp }       # fields can be renamed too
    date_created:             { date_created: timestamp }       # use alias for field if changed
    state:                    { state: varchar(5) }
    details:                  { details: text }                 # json-field saved as stringified text
    details.id:               { business_key: integer }         # conflicts can be resolved with dot notation
    details.order__id:        { order__id: integer }            # nested fields can be selected with "__"
    details.client__list:                                       # list will be placed to another table
    details.purchase__list:   { client__list: text }            # also list can be saved as stringified text
    extra**.id:               { extra_id: integer }             # text field containing valid json is accessble with "**"
    extra**.user__name:       { username: varchar(255) }        # after unpacking field is selectable as generic json
    options**:                                                  # text field with json can be unpacked to separate table

postgres.orders.options:                                        # the last field of previous specification is placed
  staging.order__options:                                       # to nested dedicated table with pk/fk relationships
    ...                                                         # generated automaticaly


# Header-style mapping
postgres.orders[date_created + state + details** + extra**]:    # use explicit list of fields to unpacking in header
  staging.orders:                                               # in this case json is merged to top level with generic fields
    id*:                      { origin_id: bigint }             # it simplifies the declaration and makes it clearer
    dt_created:               { date_created: timestamp }       # (be careful with duplicates general and json fields!)
    date_created:             { date_created: timestamp }
    state:                    { state: varchar(5) }
    order__id:                { order__id: integer }
    user_name:                { username: varchar(255) }
    ...
```


## Usage

This following example demonstrates a typical use case:

1. Use *postgres* as source system
1. Save nested tables to *mysql* database
1. Using mapping specification as shown above

```python
from airflow import DAG
from airflow.providers.normalizer.operators.normalizer import NormalizerOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="normalize_example",
    description="Normalize nested jsons to analytical database",
    start_date=days_ago(1),
    schedule="0 * * * *",
) as dag:

    # Simple normalize with mapping file
    NormalizerOperator(
        task_id="normalize_json",
        source_conn_id="postgres_default",
        destination_conn_id="mysql_default",
        mapping="mappings/mapping.yaml",
    )
```


Another example demonstrates a custom preprocessing use case with full-refresh mode:

```python
from airflow import DAG
from airflow.providers.normalizer.operators.normalizer import NormalizerOperator
from airflow.utils.dates import days_ago


def preprocessing(doc):
    """ Fix bad datetime format. """
    if "contract" in doc:
        if str(doc["contract"]["date"]) == "0001-01-01T00:00:00+00:00":
            doc["contract"]["date"] = "1970-01-01 00:00:00"
    return [doc]


with DAG(
    dag_id="normalize_example",
    description="Normalize nested jsons with custom preprocessing",
    start_date=days_ago(1),
    schedule="0 * * * *",
) as dag:

    # Simple normalize with drop tables before and preprocessing documents
    NormalizerOperator(
        task_id="normalize_json",
        source_conn_id="postgres_default",
        destination_conn_id="mysql_default",
        mapping="""
            postgres.orders[order_details + state + dt_created + dt_changed]:
              staging.orders:
                # general columns
                state:                       { state: String }
                dt_created:                  { created_date: DateTime }
                dt_changed:                  { changed_date: DateTime }
                init_system:                 { init_system: String }
                reference_id:                { reference_id: String }
                # json-fields
                client__id:                  { client__id: String }
                client__name:                { client__name: String }
                contract__id:                { contract__id: String }
                contract__date:              { contract__date: DateTime }
                contract__branch__id:        { contract__branch__id: String }
        """,
        preprocessing=preprocessing,
        commit_every=100,
    )
```


Last example demonstrates a custom SQL-templates usage with extended SQL syntax:

```python
from airflow import DAG
from airflow.providers.normalizer.operators.normalizer import NormalizerOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="normalize_example",
    description="Normalize nested jsons with custom SQL-templates",
    start_date=days_ago(1),
    schedule="0 * * * *",
) as dag:

    # Customize specific SQL-dialect in deep
    NormalizerOperator(
        task_id="normalize_json",
        source_conn_id="postgres_default",
        destination_conn_id="clickhouse_default",
        mapping="mapping/clickhouse.yaml",
        select_count_query=(
            '''
                SELECT count(id)
                FROM {table}
            '''
        ),
        select_all_query=(
            '''
                SELECT {fields}
                FROM {table}
                WHERE date_system > {{ date_system }}
            '''
        ),
        create_table_query=(
            '''
                DROP TABLE IF EXISTS {table}
            ''',
            '''
                CREATE TABLE IF NOT EXISTS {table} (
                    {definition}
                )
                ENGINE = MergeTree
                ORDER BY {fk or pk}
            ''',
        ),
        insert_into_query=(
            '''
                INSERT INTO {table} (
                    {fields}
                )
                VALUES {values}
            ''',
        ),
        incremental=True,
        retries=2,
    )
```


## NormalizerOperator Reference

To import `NormalizerOperator` use:

  `from airflow.providers.normalizer.operators.normalizer import NormalizerOperator`

Supported kwargs:
  * `source_conn_id`: source connection id that contains table with text/json/jsonb.
  * `destination_conn_id`: destination connection id where nested tables is placed.
  * `mapping`: YAML specification
  * `select_count_query`: overwrites base query used to calculate total for pagination
  * `select_all_query`: overwrites base query used to LIMIT/OFFSET pagination
  * `create_table_query`: overwrites base template query
  * `incremental`: set update mode, by default is `False`
  * Other kwargs are inherited from Airflow `BaseOperator`

In the query templates placeholders are available:
  * `{table}`: placeholder for table name
  * `{definition}`: placeholder for pairs field name and type (e.g. CREATE)
  * `{fields}`: placeholder for list of columns comma separated (e.g. SELECT)
  * `{values}`: placeholder for mulitvalue-INSERT comma separated values
  * `{pk}`: name of generated primary key
  * `{fk}`: name of generated foreign key
