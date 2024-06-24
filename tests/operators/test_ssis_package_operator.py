from unittest import TestCase

from SSIS_Operator.models.SqlQueryParameters import LoggingLevel, QueryParameters, ParameterType
from SSIS_Operator.operators.ssis_package_operator import SsisPackageOperator


class TestSsisPackageOperator(TestCase):
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
            "DECLARE @pparDatabaseName sql_variant = N'testdb01'\n"
            "EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type=20, "
            "@parameter_name=N'pparDatabaseName', @parameter_value=@pparDatabaseName\n"
            "DECLARE @pparServerName sql_variant = N'server01'\n"
            "EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type=20,"
            " @parameter_name=N'pparServerName', @parameter_value=@pparServerName\n",
            operator.sql_parameters
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
            "DECLARE @pparDatabaseName sql_variant = N'testdb01'\n"
            "EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type=20, "
            "@parameter_name=N'pparDatabaseName', @parameter_value=@pparDatabaseName\n"
            "DECLARE @pparServerName sql_variant = N'server01'\n"
            "EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type=20,"
            " @parameter_name=N'pparServerName', @parameter_value=@pparServerName\n",
            operator.sql_parameters
        )
    # def test_execute(self):
    #     self.fail()
