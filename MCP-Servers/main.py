import requests
from mcp.server.fastmcp import FastMCP
from typing import Any, Dict,Optional
import os
import base64

mcp = FastMCP("Databricks-server")

DATABRICKS_INSTANCE = os.environ.get("DATABRICKS_INSTANCE")
DATABRICKS_TOKEN=os.environ.get("DATABRICKS_TOKEN")


@mcp.resource("clusters://all")
def list_clusters() -> dict:
    """
    Returns a list of all existing Databricks clusters.
    """
    url = f"{DATABRICKS_INSTANCE}/api/2.0/clusters/list"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        clusters = response.json().get("clusters", [])
        return {
            "count": len(clusters),
            "clusters": [
                {
                    "id": cluster.get("cluster_id"),
                    "name": cluster.get("cluster_name"),
                    "state": cluster.get("state"),
                }
                for cluster in clusters
            ]
        }

    except requests.exceptions.RequestException as e:
        return {
            "error": f"Failed to fetch clusters: {str(e)}"
        }

@mcp.tool()
async def create_cluster(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new Databricks cluster using provided configuration.

    Args:
        cluster_config: Dictionary with cluster configuration.

    Returns:
        Dictionary with result (cluster ID or error message).
    """
    url = f"{DATABRICKS_INSTANCE}/api/2.0/clusters/create"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, headers=headers, json=cluster_config)
        response.raise_for_status()
        result = response.json()
        return {
            "message": "Cluster created successfully",
            "cluster_id": result.get("cluster_id", "Unknown")
        }

    except requests.exceptions.HTTPError as http_err:
        return {
            "error": f"HTTP error occurred: {http_err}",
            "details": response.text
        }
    except requests.exceptions.RequestException as req_err:
        return {
            "error": f"Request error: {req_err}"
        }
    
@mcp.tool()
async def start_cluster(cluster_id: str) -> Dict[str, Any]:
    """
    Start a stopped Databricks cluster by its cluster_id.

    Args:
        cluster_id: The ID of the cluster to start.

    Returns:
        A dictionary with the result or error message.
    """
    url = f"{DATABRICKS_INSTANCE}/api/2.0/clusters/start"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "cluster_id": cluster_id
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()

        return {
            "message": f"Cluster '{cluster_id}' start request submitted successfully."
        }

    except requests.exceptions.HTTPError as http_err:
        return {
            "error": f"HTTP error occurred: {http_err}",
            "details": response.text
        }
    except requests.exceptions.RequestException as req_err:
        return {
            "error": f"Request error: {req_err}"
        }

@mcp.resource("notebooks://all")
def list_all_notebooks() -> Dict[str, Any]:
    """
    Recursively fetch all notebooks in the Databricks workspace with their full paths.
    """
    url = f"{DATABRICKS_INSTANCE}/api/2.0/workspace/list"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }

    def fetch_notebooks_recursively(path="/"):
        notebooks = []
        try:
            response = requests.get(url, headers=headers, params={"path": path})
            response.raise_for_status()
            objects = response.json().get("objects", [])

            for obj in objects:
                if obj["object_type"] == "NOTEBOOK":
                    notebooks.append({
                        "path": obj["path"],
                        "language": obj.get("language", "unknown")
                    })
                elif obj["object_type"] == "DIRECTORY":
                    notebooks.extend(fetch_notebooks_recursively(obj["path"]))

        except requests.exceptions.RequestException as e:
            notebooks.append({"error": f"Failed to fetch from path '{path}': {str(e)}"})

        return notebooks

    all_notebooks = fetch_notebooks_recursively()
    return {
        "notebook_count": len(all_notebooks),
        "notebooks": all_notebooks
    }


@mcp.tool()
async def start_pipeline(pipeline_id: str, full_refresh: bool = False) -> Dict[str, Any]:
    """
    Starts a Delta Live Table pipeline by triggering an update.
 
    Args:
        pipeline_id: The ID of the pipeline to start.
        full_refresh: Whether to perform a full refresh (default: False).
 
    Returns:
        Dictionary with update ID or error message.
    """
    url = f"{DATABRICKS_INSTANCE}/api/2.0/pipelines/{pipeline_id}/updates"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
 
    body = {
        "full_refresh": full_refresh
    }
 
    try:
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()
        data = response.json()
        return {
            "message": "Pipeline started successfully",
            "update_id": data.get("update_id")
        }
 
    except requests.exceptions.HTTPError as http_err:
        return {
            "error": f"HTTP error occurred: {http_err}",
            "details": response.text
        }
    except requests.exceptions.RequestException as req_err:
        return {
            "error": f"Request error: {req_err}"
        }
    
@mcp.tool()
async def create_pipeline(pipeline_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Creates a Delta Live Table pipeline in Databricks.
 
    Args:
        pipeline_config: A dictionary with the required pipeline configuration.
            Required fields often include:
                - name (str): Name of the pipeline
                - storage (str): DBFS root path for storage
                - target (str): Target schema
                - libraries (list): List of notebook paths or JARs
                - clusters (list): Cluster spec
                - continuous (bool): Whether pipeline is continuous
 
    Returns:
        Dictionary containing pipeline_id or error details.
    """
    url = f"{DATABRICKS_INSTANCE}/api/2.0/pipelines"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
 
    try:
        response = requests.post(url, headers=headers, json=pipeline_config)
        response.raise_for_status()
        data = response.json()
        return {
            "message": "Pipeline created successfully",
            "pipeline_id": data.get("pipeline_id")
        }
 
    except requests.exceptions.HTTPError as http_err:
        return {
            "error": f"HTTP error occurred: {http_err}",
            "details": response.text
        }
    except requests.exceptions.RequestException as req_err:
        return {
            "error": f"Request error: {req_err}"
        }
    
def is_base64(s: str) -> bool:
    try:
        return base64.b64encode(base64.b64decode(s)).decode('utf-8') == s
    except Exception:
        return False

@mcp.tool()
async def import_notebook_mcp(
    path: str,
    content: str,
    format: str = "SOURCE",
    language: Optional[str] = None,
    overwrite: bool = False,
) -> Dict[str, Any]:
    """
    Import a notebook into the Databricks workspace.

    Args:
        path: Path to store the notebook.
        content: Base64-encoded content of the notebook.
        format: Format of the notebook (SOURCE). 
        language: Language of the notebook (PYTHON).
        overwrite: Whether to overwrite existing notebook.

    Returns:
        Dictionary with success or error information.
    """

    content = base64.b64encode(content.encode("utf-8")).decode("utf-8")

    import_data = {
        "path": path,
        "format": format,
        "content": content,
        "overwrite": overwrite
    }

    if language:
        import_data["language"] = language

    url = f"{DATABRICKS_INSTANCE}/api/2.0/workspace/import"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, headers=headers, json=import_data)
        response.raise_for_status()
        return {"message": "Notebook imported successfully", "path": path}
    
    except requests.exceptions.HTTPError as http_err:
        return {
            "error": f"HTTP error occurred: {http_err}",
            "details": response.text
        }
    except requests.exceptions.RequestException as req_err:
        return {
            "error": f"Request error: {req_err}"
        }
    

@mcp.tool()
async def upload_csv_to_dbfs(
    path: str,
    content: str,
    overwrite: bool = False,
) -> Dict[str, Any]:
    """
    Upload a CSV file to Databricks DBFS.

    Args:
        path: DBFS path to upload the CSV to (e.g., /mnt/data/filename.csv).
        content: Raw string content of the CSV (will be base64-encoded).
        overwrite: Whether to overwrite the existing file.

    Returns:
        Dictionary with success or error information.
    """

    encoded_content = base64.b64encode(content.encode("utf-8")).decode("utf-8")

    data = {
        "path": path,
        "overwrite": overwrite,
        "contents": encoded_content
    }

    url = f"{DATABRICKS_INSTANCE}/api/2.0/dbfs/put"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return {"message": "CSV uploaded to DBFS successfully", "path": path}

    except requests.exceptions.HTTPError as http_err:
        return {
            "error": f"HTTP error occurred: {http_err}",
            "details": response.text
        }
    except requests.exceptions.RequestException as req_err:
        return {
            "error": f"Request error: {req_err}"
        }




@mcp.tool()
async def create_job_workflow(job_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Creates a job workflow in Databricks using the /api/2.2/jobs/create endpoint.

    Args:
        job_config: A dictionary that includes job configuration as per the Databricks API.
            Required fields typically include:
                - name (str): Name of the job
                - format (str): e.g., MULTI_TASK
                - max_concurrent_runs (int)
                - environments (list): Python environment configs
                - tasks (list): List of tasks with details like notebook, python scripts, or dbt

    Returns:
        Dictionary with job_id or error details.
    """
    url = f"{DATABRICKS_INSTANCE}/api/2.2/jobs/create"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, headers=headers, json=job_config)
        response.raise_for_status()
        data = response.json()
        return {
            "message": "Job workflow created successfully",
            "job_id": data.get("job_id")
        }

    except requests.exceptions.HTTPError as http_err:
        return {
            "error": f"HTTP error occurred: {http_err}",
            "details": response.text
        }
    except requests.exceptions.RequestException as req_err:
        return {
            "error": f"Request error: {req_err}"
        }
    
@mcp.tool()
async def run_job_now(run_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Triggers a Databricks job immediately using the /api/2.2/jobs/run-now endpoint.

    Args:
        run_config: A dictionary containing the job run configuration.
            Expected keys include:
                - job_id (int): ID of the job to run
                - idempotency_token (str, optional): Ensures request is idempotent
                - job_parameters (dict, optional): Parameters for the job
                - only (list, optional): Specific tasks to run
                - performance_target (str, optional): PERFORMANCE_OPTIMIZED or COST_OPTIMIZED
                - pipeline_params (dict, optional): e.g., {"full_refresh": false}
                - queue (dict, optional): e.g., {"enabled": true}

    Returns:
        Dictionary with run_id if successful or error details.
    """
    url = f"{DATABRICKS_INSTANCE}/api/2.2/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, headers=headers, json=run_config)
        response.raise_for_status()
        data = response.json()
        return {
            "message": "Job run triggered successfully",
            "run_id": data.get("run_id")
        }

    except requests.exceptions.HTTPError as http_err:
        return {
            "error": f"HTTP error occurred: {http_err}",
            "details": response.text
        }
    except requests.exceptions.RequestException as req_err:
        return {
            "error": f"Request error: {req_err}"
        }
    

DEFAULT_PARAMETERS = "null"   
@mcp.tool(name="execute_sql_statement", description="Execute a SQL statement on a specified Databricks warehouse.")
async def execute_statement(
    statement: str,
    warehouse_id: str,
    catalog: Optional[str] = None,
    db_schema: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    row_limit: int = 10000,
    byte_limit: int = 26214400  # 100MB
) -> Dict[str, Any]:
    """
    Executes a SQL query on a Databricks SQL warehouse using REST API.
 
    Args:
        statement: The SQL statement to execute.
        warehouse_id: ID of the SQL warehouse to use.
        catalog: Optional catalog to use.
        schema: Optional schema to use.
        parameters: Optional statement parameters.
        row_limit: Maximum number of rows to return.
        byte_limit: Maximum number of bytes to return.
 
    Returns:
        A dictionary with the response data or error message.
    """
    url = f"{DATABRICKS_INSTANCE}/api/2.0/sql/statements"
 
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
 
    request_data = {
        "statement": statement,
        "warehouse_id": warehouse_id,
        "wait_timeout": "10s",
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
        "row_limit": row_limit,
        "byte_limit": byte_limit
    }
 
    if catalog:
        request_data["catalog"] = catalog
    if db_schema:
        request_data["schema"] = db_schema
    if parameters:
        request_data["parameters"] = parameters
 
    try:
        response = requests.post(url, headers=headers, json=request_data)
        response.raise_for_status()
        return response.json()
 
    except requests.exceptions.HTTPError as http_err:
        return {
            "error": f"HTTP error occurred: {http_err}",
            "details": response.text
        }
    except requests.exceptions.RequestException as req_err:
        return {
            "error": f"Request failed: {req_err}"
        }
 
async def execute_statement_prefilled(
    statement: str,
    warehouse_id: str,
    catalog: Optional[str] = None,
    db_schema: Optional[str] = None,
    row_limit: int = 10000,
    byte_limit: int = 26214400
) -> Dict[str, Any]:
    return await execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        catalog=catalog,
        db_schema=db_schema,
        parameters=DEFAULT_PARAMETERS,
        row_limit=row_limit,
        byte_limit=byte_limit
    )
 
@mcp.tool()
async def list_dbfs_directory(path: str) -> Dict[str, Any]:
    """
    List the contents of a DBFS directory or get file details.
 
    Args:
        path: Absolute DBFS path of the directory or file to list (e.g., /mnt/foo).
 
    Returns:
        Dictionary with file list or error information.
    """
    url = f"{DATABRICKS_INSTANCE}/api/2.0/dbfs/list"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }
    params = {
        "path": path
    }
 
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        return {
            "message": "Directory contents retrieved successfully",
            "files": data.get("files", []),
            "path": path
        }
 
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 404:
            return {
                "error": "RESOURCE_DOES_NOT_EXIST",
                "message": f"The path '{path}' does not exist.",
                "details": response.text
            }
        return {
            "error": f"HTTP error occurred: {http_err}",
            "details": response.text
        }
 
    except requests.exceptions.RequestException as req_err:
        return {
            "error": f"Request error: {req_err}"
        }
 