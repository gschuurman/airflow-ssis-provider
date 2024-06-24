from unittest import TestCase

from SSIS_Operator.models.SqlQueryParameters import LoggingLevel, QueryParameters, ParameterType
from SSIS_Operator.operators.ssis_package_operator import SsisPackageOperator


class TestSsisPackageOperator(TestCase):
    def test_sql_reference_formatting(self):
        params = [
            QueryParameters('pparDatabaseName', 'testdb01', ParameterType.PROJECT),
            QueryParameters('pparServerName', 'server01', ParameterType.PROJECT)
        ]

        operator = SsisPackageOperator(
            task_id='task1',
            conn_id='test_id',
            database='SSISDB',
            folder='folder1',
            project='project1',
            package='package1',
            environment='environment1',
            logging_level=LoggingLevel.runtime_lineage,
            parameters=params
        )

        self.assertEqual(
            """DECLARE @reference_id BIGINT = (SELECT er.[reference_id]
            FROM [SSISDB].[catalog].[environment_references] er
            LEFT JOIN [SSISDB].[catalog].[projects] p ON er.project_id = p.project_id
            LEFT JOIN [SSISDB].[catalog].[folders] f ON p.folder_id = f.folder_id
            WHERE f.name = N'folder1'
                AND p.name = N'project1'
                AND er.environment_name = N'environment1')""",
            operator.sql_reference_query
        )
        self.assertEqual(
            f"\n{' ' * 8},@reference_id = @reference_id",
            operator.sql_reference_parameter
        )

    def test_sql_parameter_formatting(self):
        params = [
            QueryParameters('pparDatabaseName', 'testdb01', ParameterType.PROJECT),
            QueryParameters('pparServerName', 'server01', ParameterType.PROJECT)
        ]

        operator = SsisPackageOperator(
            task_id='task1',
            conn_id='test_id',
            database='SSISDB',
            folder='folder1',
            project='project1',
            package='package1',
            environment='environment1',
            logging_level=LoggingLevel.runtime_lineage,
            parameters=params
        )

        self.assertEqual(
            """
    DECLARE @pparDatabaseName sql_variant = N'testdb01'
    EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type=20, @parameter_name=N'pparDatabaseName', @parameter_value=@pparDatabaseName

    DECLARE @pparServerName sql_variant = N'server01'
    EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type=20, @parameter_name=N'pparServerName', @parameter_value=@pparServerName
""",
            operator.sql_parameters
        )

    def test_sql_formatting(self):
        params = [
            QueryParameters('pparDatabaseName', 'testdb01', ParameterType.PROJECT),
            QueryParameters('pparServerName', 'server01', ParameterType.PROJECT)
        ]

        operator = SsisPackageOperator(
            task_id='task1',
            conn_id='test_id',
            database='SSISDB',
            folder='folder1',
            project='project1',
            package='package1',
            environment='environment1',
            logging_level=LoggingLevel.runtime_lineage,
            parameters=params
        )

        self.assertEqual(
            """
    DECLARE @execution_id BIGINT
    DECLARE @reference_id BIGINT = (SELECT er.[reference_id]
            FROM [SSISDB].[catalog].[environment_references] er
            LEFT JOIN [SSISDB].[catalog].[projects] p ON er.project_id = p.project_id
            LEFT JOIN [SSISDB].[catalog].[folders] f ON p.folder_id = f.folder_id
            WHERE f.name = N'folder1'
                AND p.name = N'project1'
                AND er.environment_name = N'environment1')
    EXEC [SSISDB].[catalog].[create_execution] 
        @folder_name = N'folder1'
        ,@project_name = N'project1'
        ,@package_name = N'package1'
        ,@use32bitruntime = False 
        ,@reference_id = @reference_id
        ,@execution_id = @execution_id OUTPUT;
    
    
    DECLARE @pparDatabaseName sql_variant = N'testdb01'
    EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type=20, @parameter_name=N'pparDatabaseName', @parameter_value=@pparDatabaseName

    DECLARE @pparServerName sql_variant = N'server01'
    EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type=20, @parameter_name=N'pparServerName', @parameter_value=@pparServerName

    
    DECLARE @LoggingLevel sql_variant = N'4'  
    EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type=50, @parameter_name=N'LOGGING_LEVEL', @parameter_value=@LoggingLevel;
       
    EXEC [SSISDB].[catalog].[start_execution] @execution_id;
    SELECT @execution_id
    """,
            operator.sql
        )
