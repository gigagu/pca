from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class ExternalTableLoadSensor(BaseSensorOperator):
    """
    Sensor to check whether a given table load entry exists in a PostgreSQL table.

    Example:
        sensor = ExternalTableLoadSensor(
            task_id="wait_for_table_load",
            postgres_conn_id="my_postgres",
            table="external_load_status",
            table_name="sales_fact",
            source_system="SAP",
            country="JP",
            business_date="2025-11-11"
        )
    """

    @apply_defaults
    def __init__(
        self,
        postgres_conn_id: str,
        table: str,
        table_name: str,
        source_system: str,
        country: str,
        business_date,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.table_name = table_name
        self.source_system = source_system
        self.country = country
        self.business_date = business_date

    def poke(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        sql = f"""
        SELECT 1
        FROM {self.table}
        WHERE table_name = %s
          AND source_system = %s
          AND country = %s
          AND business_date = %s
        LIMIT 1;
        """

        self.log.info(
            "Checking if entry exists in %s for table_name=%s, source_system=%s, country=%s, business_date=%s",
            self.table,
            self.table_name,
            self.source_system,
            self.country,
            self.business_date,
        )

        records = hook.get_records(
            sql, parameters=(self.table_name, self.source_system, self.country, self.business_date)
        )

        if records:
            self.log.info("✅ Entry found in %s. Sensor successful.", self.table)
            return True
        else:
            self.log.info("⏳ Entry not found yet. Sensor will retry.")
            return False
