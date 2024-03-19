from airflow.models import BaseOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.decorators import apply_defaults


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
        ,@execution_id = @execution_id OUTPUT
            
    EXEC [SSISDB].[catalog].[start_execution] @execution_id;
    SELECT @execution_id
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

    def execute(self, context):
        sqlserver_hook = MsSqlHook(
            mssql_conn_id=self.conn_id,
            schema=self.database
        )

        sql = SsisPackageOperator.sql_query.format(
            folder=self.folder,
            project=self.project,
            package=self.package,
            environment=self.environment
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
