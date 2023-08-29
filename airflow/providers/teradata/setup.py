import setuptools

__version__ = "1.0.0"

setuptools.setup(
    name="apache-airflow-providers-teradata",
    packages=setuptools.find_packages(),
    description="Interact with Teradata SQL Database Server from Airflow via teradatasql driver",
    entry_points={"airflow.plugins": ["teradata_hook = airflow.providers.teradata.hooks.teradata:TeradataHook"]},
    python_requires=">=3.9",
    install_requires=["teradatasql", "teradatasqlalchemy", "pandas", "apache-airflow[celery]"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Linux",
    ],
    version=__version__,
)
