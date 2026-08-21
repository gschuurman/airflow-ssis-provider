"""Manifest of modules worth generating API reference pages for.

Consumed by `product-site/docs/generate_autodoc_includes.py` via
`from SSIS_Operator._autodoc import AUTODOC_TARGETS`. Kept in this package so
the module list stays in sync with the package itself rather than a
hand-maintained copy in the docs repo.
"""

AUTODOC_TARGETS: list[str] = [
    "SSIS_Operator.models.SqlQueryParameters",
    "SSIS_Operator.operators.ssis_package_operator",
    "SSIS_Operator.sensors.ssis_package_sensor",
]
