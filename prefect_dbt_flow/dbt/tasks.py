"""Code for generate prefect DAG, includes dbt run and test functions"""
import json
import re
from datetime import datetime
from typing import Dict, List, Optional

from prefect import Task, get_run_logger, task
from prefect.futures import PrefectFuture
from prefect.artifacts import create_table_artifact

from prefect_dbt_flow.dbt import (
    DbtDagOptions,
    DbtNode,
    DbtProfile,
    DbtProject,
    DbtResourceType,
    cli,
)
from prefect_dbt_flow.dbt.profile import override_profile

DBT_RUN_EMOJI = "🏃"
DBT_TEST_EMOJI = "🧪"
DBT_SEED_EMOJI = "🌱"
DBT_SNAPSHOT_EMOJI = "📸"


def _task_dbt_snapshot(
    project: DbtProject,
    profile: Optional[DbtProfile],
    dag_options: Optional[DbtDagOptions],
    dbt_node: DbtNode,
    task_kwargs: Optional[Dict] = None,
) -> Task:
    """
    Create a Prefect task for running a dbt snapshot. Uses dbt_snapshot from cli module

    Args:
        project: A class that represents a dbt project configuration.
        profile: A class that represents a dbt profile configuration.
        dag_options: A class to add dbt DAG configurations.
        dbt_node: A class that represents the dbt node (model) to run.
        task_kwargs: Additional task configuration.

    Returns:
        dbt_snapshot: Prefect task.
    """
    all_task_kwargs = {
        **(task_kwargs or {}),
        "name": f"{DBT_SNAPSHOT_EMOJI} snapshot_{dbt_node.name}",
    }

    @task(**all_task_kwargs)
    def dbt_snapshot():
        """
        Snapshots a dbt snapshot

        Returns:
            None
        """
        with override_profile(project, profile) as _project:
            if not dag_options or dag_options.install_deps:
                dbt_deps_output = cli.dbt_deps(_project, profile, dag_options)
                get_run_logger().info(dbt_deps_output)

            dbt_snapshot_output = cli.dbt_snapshot(
                _project, dbt_node.name, profile, dag_options
            )
            get_run_logger().info(dbt_snapshot_output)

    return dbt_snapshot


def _task_dbt_seed(
    project: DbtProject,
    profile: Optional[DbtProfile],
    dag_options: Optional[DbtDagOptions],
    dbt_node: DbtNode,
    task_kwargs: Optional[Dict] = None,
) -> Task:
    """
    Create a Prefect task for running a dbt seed. Uses dbt_seed from cli module

    Args:
        project: A class that represents a dbt project configuration.
        profile: A class that represents a dbt profile configuration.
        dag_options: A class to add dbt DAG configurations.
        dbt_node: A class that represents the dbt node (model) to run.
        task_kwargs: Additional task configuration.

    Returns:
        dbt_seed: Prefect task.
    """
    all_task_kwargs = {
        **(task_kwargs or {}),
        "name": f"{DBT_SEED_EMOJI} seed_{dbt_node.name}",
    }

    @task(**all_task_kwargs)
    def dbt_seed():
        """
        Seeds a dbt seed

        Returns:
            None
        """
        with override_profile(project, profile) as _project:
            if not dag_options or dag_options.install_deps:
                dbt_deps_output = cli.dbt_deps(_project, profile, dag_options)
                get_run_logger().info(dbt_deps_output)

            dbt_seed_output = cli.dbt_seed(
                _project, dbt_node.name, profile, dag_options
            )
            get_run_logger().info(dbt_seed_output)

    return dbt_seed


def _task_dbt_run(
    project: DbtProject,
    profile: Optional[DbtProfile],
    dag_options: Optional[DbtDagOptions],
    dbt_node: DbtNode,
    task_kwargs: Optional[Dict] = None,
) -> Task:
    """
    Create a Prefect task for running a dbt model. Uses dbt_run from cli module

    Args:
        project: A class that represents a dbt project configuration.
        profile: A class that represents a dbt profile configuration.
        dag_options: A class to add dbt DAG configurations.
        dbt_node: A class that represents the dbt node (model) to run.
        task_kwargs: Additional task configuration.

    Returns:
        dbt_run: A prefect task.
    """
    all_task_kwargs = {
        **(task_kwargs or {}),
        "name": f"{DBT_RUN_EMOJI} {dbt_node.name}",
    }

    @task(**all_task_kwargs)
    def dbt_run():
        """
        Run a dbt model.

        Returns:
            None
        """
        with override_profile(project, profile) as _project:
            if not dag_options or dag_options.install_deps:
                dbt_deps_output = cli.dbt_deps(_project, profile, dag_options)
                get_run_logger().info(dbt_deps_output)

            dbt_run_output = cli.dbt_run(_project, dbt_node.name, profile, dag_options)
            get_run_logger().info(dbt_run_output)

    return dbt_run


def _parse_test_results(test_output: str, test_name: str) -> List[Dict]:
    """Convert dbt show JSON output into a list of records"""
    logger = get_run_logger()

    try:
        # Find the start of the JSON output (line ending with '{')
        json_lines = [line.strip() for line in test_output.split('\n') if line.strip()]

        # Since the line ending with '{' is the start of the JSON output, but also contains a timestamp,
        # we start at the next line, and just prepend the '{' to the start of the JSON string
        json_start = next(i for i, line in enumerate(json_lines) if line.endswith('{')) + 1

        # Combine all lines from json_start onwards into a single string
        json_str = "{"+''.join(json_lines[json_start+1:])
        json_data = json.loads(json_str)
        results = json_data.get("show", [])
        # Add source test name to each result
        for result in results:
            result['_source_test'] = test_name
        logger.info(f"Parsed {len(results)} results from test output for {test_name}")
        return results
    except (json.JSONDecodeError, StopIteration, KeyError) as e:
        logger.error(f"Failed to parse test output: {str(e)}")
        return [{"error": "Failed to parse test output", "raw_output": test_output, "_source_test": test_name}]


def _save_test_failure(test_name: str, test_output: str, project_name: str):
    """Helper function to save test failure results as Prefect artifacts"""
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    
    # Parse the test output into tabular format
    table_data = _parse_test_results(test_output, test_name)
    
    # Create table artifact with the test results
    table_key = f"{project_name}-{test_name}-{timestamp}".replace("_","-")
    get_run_logger().info(f"Saving test failure results as Prefect artifact: {table_key}")
    create_table_artifact(
        key=table_key,
        table=table_data,
        description=f"Test failure results for {test_name}"
    )
    
    return table_key

def _extract_failed_test_names(exception_text: str) -> List[str]:
    """Extract names of failing tests from dbt output"""
    pattern = r"FAIL\s\d*\s(\w+)"
    return re.findall(pattern, exception_text)

def _task_dbt_test(
    project: DbtProject,
    profile: Optional[DbtProfile],
    dag_options: Optional[DbtDagOptions],
    dbt_node: DbtNode,
    task_kwargs: Optional[Dict] = None,
) -> Task:
    """
    Create a Prefect task for testing a dbt model. Uses dbt_test from cli module

    Args:
        project: A class that represents a dbt project configuration.
        profile: A class that represents a dbt profile configuration.
        dag_options: A class to add dbt DAG configurations.
        dbt_node: A class that represents the dbt node (model) to run.
        task_kwargs: Additional task configuration.

    Returns:
        dbt_test: Prefect task.
    """
    all_task_kwargs = {
        **(task_kwargs or {}),
        "name": f"{DBT_TEST_EMOJI} test_{dbt_node.name}",
    }

    @task(**all_task_kwargs)
    def dbt_test():
        """
        Test a dbt model

        Returns:
            List[Dict]: List of test failure results if any tests failed
        """
        logger = get_run_logger()
        test_results = []
        with override_profile(project, profile) as _project:
            if not dag_options or dag_options.install_deps:
                dbt_deps_output = cli.dbt_deps(_project, profile, dag_options)
                get_run_logger().info(dbt_deps_output)

            try:
                dbt_test_output = cli.dbt_test(
                    _project, dbt_node.name, profile, dag_options
                )
                get_run_logger().info(dbt_test_output)
                return test_results  # Return empty list if no failures
            except Exception as e:
                # Extract failing test names from exception
                failed_tests = _extract_failed_test_names(str(e))
                logger.info(f"Found failing tests: {failed_tests}")
                
                # Get detailed results for each failing test
                for test_name in failed_tests:
                    try:
                        test_results_output = cli.dbt_show(
                            _project,
                            test_name,  # Use specific test name
                            profile,
                            dag_options,
                            limit=dag_options.dbt_show_limit,
                        )
                        
                        table_data = _parse_test_results(test_results_output, test_name)
                        
                        test_results.extend(table_data)
                        
                        # Save results as Prefect artifacts
                        artifact_key = _save_test_failure(
                            test_name,
                            test_results_output,
                            _project.name
                        )
                        get_run_logger().warning(f"Test failure details saved as Prefect artifact: {artifact_key}")
                    except Exception as show_error:
                        logger.error(f"Failed to capture details for test {test_name}: {str(show_error)}")

                if dag_options and dag_options.log_test_failures_as_warnings:
                    logger.warning(f"Alert {dbt_node.name} triggered: {str(e)}")
                    return test_results
                else:
                    raise

    return dbt_test


RESOURCE_TYPE_TO_TASK = {
    DbtResourceType.SEED: _task_dbt_seed,
    DbtResourceType.MODEL: _task_dbt_run,
    DbtResourceType.SNAPSHOT: _task_dbt_snapshot,
    DbtResourceType.TEST: _task_dbt_test,  # Add TEST mapping
}


def generate_tasks_dag(
    project: DbtProject,
    profile: Optional[DbtProfile],
    dag_options: Optional[DbtDagOptions],
    dbt_graph: List[DbtNode],
    run_test_after_model: bool = False,
) -> List[PrefectFuture]:
    logger = get_run_logger()
    
    all_tasks = {
        dbt_node.unique_id: RESOURCE_TYPE_TO_TASK[dbt_node.resource_type](
            project=project,
            profile=profile,
            dag_options=dag_options,
            dbt_node=dbt_node,
        )
        for dbt_node in dbt_graph
    }

    test_nodes = [node for node in dbt_graph if node.resource_type == DbtResourceType.TEST]
    logger.info(f"Found {len(test_nodes)} test nodes in graph")

    test_futures = []
    submitted_tasks: Dict[str, PrefectFuture] = {}
    
    while node := _get_next_node(dbt_graph, list(submitted_tasks.keys())):
        run_task = all_tasks[node.unique_id]
        task_dependencies = [
            submitted_tasks[node_unique_id] for node_unique_id in node.depends_on
        ]

        task_future = run_task.submit(wait_for=task_dependencies)

        if run_test_after_model and node.has_tests:
            test_task = _task_dbt_test(
                project=project,
                profile=profile,
                dag_options=dag_options,
                dbt_node=node,
            )
            test_task_future = test_task.submit(wait_for=task_future)

            submitted_tasks[node.unique_id] = test_task_future
        else:
            submitted_tasks[node.unique_id] = task_future
        
        if node.resource_type == DbtResourceType.TEST:
            test_futures.append(task_future)

    return test_futures


def _get_next_node(
    dbt_graph: List[DbtNode], submitted_tasks: List[str]
) -> Optional[DbtNode]:
    for node in dbt_graph:
        if node.unique_id in submitted_tasks:
            continue

        node_dependencies = [node_unique_id for node_unique_id in node.depends_on]
        if set(node_dependencies) <= set(submitted_tasks):
            return node

    return None
