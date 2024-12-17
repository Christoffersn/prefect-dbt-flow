"""Functions to create a prefect flow for a dbt project."""
from typing import Optional

from prefect import Flow, flow

from prefect_dbt_flow.dbt import DbtDagOptions, DbtProfile, DbtProject, graph, tasks


def dbt_flow(
    project: DbtProject,
    profile: Optional[DbtProfile] = None,
    dag_options: Optional[DbtDagOptions] = None,
    flow_name: Optional[str] = None,
    flow_kwargs: Optional[dict] = None,
) -> Flow:
    """
    Create a PrefectFlow for executing a dbt project.

    Args:
        project: A Class that represents a dbt project configuration.
        profile: A Class that represents a dbt profile configuration.
        dag_options: A Class to add dbt DAG configurations.
        flow_kwargs: A dict of prefect @flow arguments

    Returns:
        dbt_flow: A Prefect Flow.
    """
    all_flow_kwargs = {
        "name": flow_name or project.name,
        **(flow_kwargs or {}),
    }

    dbt_graph = graph.parse_dbt_project(project, profile, dag_options)

    @flow(**all_flow_kwargs)
    def dbt_flow():
        """
        Function that configures and runs a Prefect flow.

        Returns:
            List[Dict]: List of test failure results from any failed tests
        """
        if dag_options and dag_options.log_test_failures_as_warnings:
            test_futures = tasks.generate_tasks_dag(
                project,
                profile,
                dag_options,
                dbt_graph,
                dag_options.run_test_after_model if dag_options else False,
            )
            
            # Collect all test results if we are logging test failures as warnings, thus not raising exceptions
            # This is useful since the parent flow will then get all test results and can log them/handle them as needed
            all_test_results = []
            if dag_options and dag_options.log_test_failures_as_warnings:
                for future in test_futures:
                    results = future.result()
                    if results:
                        all_test_results.extend(results)
                    
            return all_test_results
        
        else:
            tasks.generate_tasks_dag(
                project,
                profile,
                dag_options,
                dbt_graph,
                dag_options.run_test_after_model if dag_options else False,
            )

    return dbt_flow