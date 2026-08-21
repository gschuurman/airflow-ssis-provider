"""Operator that starts an SSIS Catalog package execution over an MSSQL connection."""

from typing import Optional, List

from SSIS_Operator.models.SqlQueryParameters import QueryParameters, LoggingLevel
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.sdk import BaseOperator


class SsisPackageOperator(BaseOperator):
    """Starts an SSIS Catalog package execution and pushes its execution ID to XCom.

    Builds a `[SSISDB].[catalog]` T-SQL script that creates and starts the
    execution, optionally binding an environment reference and setting
    project/package/logging parameters, then runs it via `MsSqlHook`. The
    resulting `execution_id` is pushed to XCom under the `execution_id` key
    so a downstream `SsisPackageSensor` can poll it.

    Example::

        SsisPackageOperator(
            task_id="run_etl_package",
            conn_id="ssisdb_default",
            database="SSISDB",
            folder="MyFolder",
            project="MyProject",
            package="LoadOrders.dtsx",
            environment="Production",
            logging_level=LoggingLevel.basic,
            parameters=[
                QueryParameters(name="BatchDate", value="2026-08-21", type=ParameterType.PACKAGE),
            ],
        )
    """
    sql_query = """
    DECLARE @execution_id BIGINT
    {reference_query}
    EXEC [SSISDB].[catalog].[create_execution] 
        @folder_name = N'{folder}'
        ,@project_name = N'{project}'
        ,@package_name = N'{package}'
        ,@use32bitruntime = False {reference_parameter}
        ,@execution_id = @execution_id OUTPUT;
    
    {sql_parameters}
    
    DECLARE @LoggingLevel sql_variant = {logging_level}  
    EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type=50, @parameter_name=N'LOGGING_LEVEL', @parameter_value=@LoggingLevel;
       
    EXEC [SSISDB].[catalog].[start_execution] @execution_id;
    SELECT @execution_id
    """

    sql_query_parameter = """
    DECLARE @{parameter_name} sql_variant = N'{parameter_value}'
    EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type={parameter_type}, @parameter_name=N'{parameter_name}', @parameter_value=@{parameter_name}
"""

    sql_query_reference = """DECLARE @reference_id BIGINT = (SELECT er.[reference_id]
            FROM [SSISDB].[catalog].[environment_references] er
            LEFT JOIN [SSISDB].[catalog].[projects] p ON er.project_id = p.project_id
            LEFT JOIN [SSISDB].[catalog].[folders] f ON p.folder_id = f.folder_id
            WHERE f.name = N'{folder}'
                AND p.name = N'{project}'
                AND er.environment_name = N'{environment}')"""

    sql_reference_parameter = ""

    def __init__(
            self,
            conn_id,
            database: str,
            folder: str,
            project: str,
            package: str,
            environment: Optional[str] = None,
            logging_level: LoggingLevel = LoggingLevel.basic,
            parameters: Optional[List[QueryParameters]] = None,
            *args,
            **kwargs
    ):
        """Initialize the operator and pre-build the SSIS Catalog execution SQL.

        Args:
            conn_id: Airflow connection ID for the target MSSQL server hosting SSISDB.
            database: Database name to connect to (typically `SSISDB`).
            folder: SSIS Catalog folder containing the project.
            project: SSIS Catalog project name.
            package: Package file name within the project, e.g. `LoadOrders.dtsx`.
            environment: Optional SSIS Catalog environment name to bind as a reference.
            logging_level: `LoggingLevel` to set for the execution. Defaults to `LoggingLevel.basic`.
            parameters: Optional list of `QueryParameters` to set before starting the execution.
            *args: Additional positional arguments passed to `BaseOperator`.
            **kwargs: Additional keyword arguments passed to `BaseOperator`.
        """
        super(SsisPackageOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.database = database
        self.folder = folder
        self.project = project
        self.package = package
        self.environment = environment
        self.sql_parameters = ''
        self.sql_reference_query = ''
        self.logging_level = logging_level
        if parameters:
            self.__build_query_parameters(parameters=parameters)
        if environment:
            self.__build_query_reference()
            self.sql_reference_parameter = f"\n{' ' * 8},@reference_id = @reference_id"
        self.__build_sql_query()

    def __build_query_parameters(self, parameters: list[QueryParameters]):
        """Append one `set_execution_parameter_value` statement per parameter to `self.sql_parameters`."""
        for parameter in parameters:
            self.sql_parameters += SsisPackageOperator.sql_query_parameter.format(
                parameter_name=parameter.name.replace("'", "''"),
                parameter_value=parameter.value.replace("'", "''"),
                parameter_type=parameter.type.value
            )

    def __build_query_reference(self):
        """Build the `@reference_id` lookup subquery for the configured environment."""
        self.sql_reference_query = SsisPackageOperator.sql_query_reference.format(
            folder=self.folder,
            project=self.project,
            environment=self.environment
        )

    def __build_sql_query(self):
        """Assemble the final `create_execution`/`start_execution` script into `self.sql`."""
        self.sql = SsisPackageOperator.sql_query.format(
            folder=self.folder,
            project=self.project,
            package=self.package,
            environment=self.environment,
            reference_query=self.sql_reference_query,
            reference_parameter=self.sql_reference_parameter,
            sql_parameters=self.sql_parameters,
            logging_level=self.logging_level.value
        )

    def execute(self, context):
        """Start the SSIS package execution and push its `execution_id` to XCom.

        Args:
            context: Airflow task instance context.

        Raises:
            ValueError: If the SSIS Catalog query returns no execution ID.
        """
        sqlserver_hook = MsSqlHook(
            mssql_conn_id=self.conn_id,
            schema=self.database
        )

        self.log.info(f"Running package using SQL: \n {self.sql}")

        result = sqlserver_hook.get_first(self.sql)

        if not result or len(result) < 1:
            self.log.info(result)
            raise ValueError("No execution ID was returned")

        context["ti"].xcom_push(
            key="execution_id",
            value=result[0]
        )
