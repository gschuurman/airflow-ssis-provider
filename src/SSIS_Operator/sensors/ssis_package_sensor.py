"""Sensor that polls the SSIS Catalog for the completion status of a package execution."""

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.sdk import BaseSensorOperator


class PackageExecutionError(Exception):
    """Raised when an SSIS Catalog execution terminates in a `Failure` or `Canceled` state."""

    def __init__(self, message, package_name, execution_status):
        """Initialize the error with the execution's terminal status.

        Args:
            message: Human-readable description of the failure.
            package_name: Name of the SSIS package that failed, as reported by the catalog.
            execution_status: Terminal status string from `SSISDB.catalog.executions` (e.g. `Failure`).
        """
        super(PackageExecutionError, self).__init__(
            message, package_name, execution_status
        )
        self.message = message
        self.package_name = package_name
        self.execution_status = execution_status


class SsisPackageSensor(BaseSensorOperator):
    """Polls `SSISDB.catalog.executions` until an SSIS package execution reaches a terminal state.

    Reads the `execution_id` pushed to XCom by an upstream `SsisPackageOperator`
    task and pokes the SSIS Catalog until the execution reaches a terminal
    status (`Success`, `Failure`, `Canceled`, `Completed`, `Pending`, or
    `Stopping`). Raises `PackageExecutionError` if the execution ends in
    `Failure` or `Canceled`.

    Example::

        SsisPackageSensor(
            task_id="wait_for_etl_package",
            conn_id="ssisdb_default",
            database="SSISDB",
            xcom_task_id="run_etl_package",
        )
    """
    sql_query = """
                SELECT CASE
                           WHEN status = 1 THEN 'Created'
                           WHEN status = 2 THEN 'Running'
                           WHEN status = 3 THEN 'Canceled'
                           WHEN status IN (4, 6) THEN 'Failure'
                           WHEN status = 5 THEN 'Pending'
                           WHEN status = 7 THEN 'Success'
                           WHEN status = 8 THEN 'Stopping'
                           WHEN status = 9 THEN 'Completed'
                           ELSE 'Failure' END AS [status_desc],
            package_name
                FROM SSISDB.catalog.executions
                WHERE execution_id = {execution_id}
                ORDER BY created_time DESC \
                """

    def __init__(
            self,
            conn_id,
            database,
            parameters=None,
            xcom_task_id=None,
            *args,
            **kwargs,
    ):
        """Initialize the sensor.

        Args:
            conn_id: Airflow connection ID for the target MSSQL server hosting SSISDB.
            database: Database name to connect to (typically `SSISDB`).
            parameters: Unused; reserved for future poke-query parameterization.
            xcom_task_id: Task ID of the upstream `SsisPackageOperator` whose `execution_id`
                XCom value should be polled.
            *args: Additional positional arguments passed to `BaseSensorOperator`.
            **kwargs: Additional keyword arguments passed to `BaseSensorOperator`.
        """
        super(SsisPackageSensor, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.database = database
        self.parameters = parameters
        self.xcom_task_id = xcom_task_id

    def poke(self, context):
        """Check whether the SSIS package execution has reached a terminal state.

        Args:
            context: Airflow task instance context.

        Returns:
            `True` if the execution has reached a terminal status, `False` if it should be polled again.

        Raises:
            PackageExecutionError: If the execution terminated with status `Failure` or `Canceled`.
        """
        hook = MsSqlHook(
            mssql_conn_id=self.conn_id,
            schema=self.database
        )

        execution_id = context["task_instance"].xcom_pull(
            self.xcom_task_id, key="execution_id"
        )

        self.log.info(
            "Poking: %s (with execution_id %s)", self.conn_id, execution_id
        )

        records = hook.get_first(
            self.sql_query.format(execution_id=execution_id)
        )

        if not records:
            return False

        self.log.info(f"Current status: {records[0]}")

        termination_flag = records[0] in (
            "Canceled",
            "Completed",
            "Failure",
            "Pending",
            "Stopping",
            "Success",
        )

        if termination_flag:
            context["ti"].xcom_push(
                key="execution_status",
                value=records[0],
            )
            context["ti"].xcom_push(
                key="package_name",
                value=records[1]
            )

        if records[0] in ("Failure", 'Canceled'):
            raise PackageExecutionError(
                message="Package execution ended abnormally",
                package_name=records[1],
                execution_status=records[0],
            )

        return termination_flag
