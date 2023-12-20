import pytest
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.normalizer.operators.normalizer import NormalizerOperator
from airflow.exceptions import AirflowFailException


def test_normalize_success():
    insert = '{"date": "2023-01-16"}'

    prepare = SqliteOperator(
        task_id="test",
        sql=insert
    )

    result = prepare.execute({})
    assert result == insert

    operator = NormalizerOperator(
        task_id="test",
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
        """
    )

    with pytest.raises(AirflowFailException):
        operator.execute({})
