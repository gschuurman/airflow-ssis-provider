from typing import Optional, List

from airflow.models import BaseOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.decorators import apply_defaults

from SSIS_Operator.models.SqlQueryParameters import QueryParameters, LoggingLevel


class SsisPackageOperator(BaseOperator):
    sql_query = """
    DECLARE @execution_id BIGINT
    DECLARE @reference_id BIGINT = (SELECT er.[reference_id]
            FROM [SSISDB].[catalog].[environment_references] er
            LEFT JOIN [SSISDB].[catalog].[projects] p ON er.project_id = p.project_id
            LEFT JOIN [SSISDB].[catalog].[folders] f ON p.folder_id = f.folder_id
            WHERE f.name = N'{folder}'
                AND p.name = N'{project}'
                AND er.environment_name = N'{environment}')
    EXEC [SSISDB].[catalog].[create_execution] 
        @folder_name = N'{folder}'
        ,@project_name = N'{project}'
        ,@package_name = N'{package}'
        ,@use32bitruntime = False
        ,@reference_id = @reference_id
        ,@execution_id = @execution_id OUTPUT;
    
    {sql_parameters}
    
    DECLARE @LoggingLevel sql_variant = N'{logging_level}'  
    EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type=50, @parameter_name=N'LOGGING_LEVEL', @parameter_value=@LoggingLevel;
       
    EXEC [SSISDB].[catalog].[start_execution] @execution_id;
    SELECT @execution_id
    """
    sql_query_parameter = """DECLARE @{parameter_name} sql_variant = N'{parameter_value}'
EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type={parameter_type}, @parameter_name=N'{parameter_name}', @parameter_value=@{parameter_name}
"""

    @apply_defaults
    def __init__(
            self,
            conn_id,
            database: str,
            folder: str,
            project: str,
            package: str,
            environment: str,
            logging_level: LoggingLevel = LoggingLevel.basic,
            parameters: Optional[List[QueryParameters]] = None,
            *args,
            **kwargs
    ):
        super(SsisPackageOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.database = database
        self.folder = folder
        self.project = project
        self.package = package
        self.environment = environment
        self.sql_parameters = ''
        self.logging_level = logging_level
        if parameters:
            self.__build_query_parameters(parameters=parameters)

    def __build_query_parameters(self, parameters: list[QueryParameters]):
        for parameter in parameters:
            self.sql_parameters += SsisPackageOperator.sql_query_parameter.format(
                parameter_name=parameter.name,
                parameter_value=parameter.value,
                parameter_type=parameter.type.value
            )

    def execute(self, context):
        sqlserver_hook = MsSqlHook(
            mssql_conn_id=self.conn_id,
            schema=self.database
        )

        sql = SsisPackageOperator.sql_query.format(
            folder=self.folder,
            project=self.project,
            package=self.package,
            environment=self.environment,
            sql_parameters=self.sql_parameters,
            logging_level=self.logging_level.value
        )

        self.log.info(f"Running package using SQL: \n {sql}")

        result = sqlserver_hook.get_first(sql)

        if not result or len(result) < 1:
            self.log.info(result)
            raise ValueError("No execution ID was returned")

        self.xcom_push(
            context=context,
            key="execution_id",
            value=result[0]
        )
