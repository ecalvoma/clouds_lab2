import azure.functions as func
import azure.durable_functions as df
import logging
from collections import defaultdict

# Initialize the Durable Functions client
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# HTTP trigger to start the MapReduce orchestration
@app.route(route="orchestrators/{functionName}")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client) -> func.HttpResponse:
    """
    HTTP trigger to start the MapReduce orchestration.
    """
    function_name = req.route_params.get('functionName')
    instance_id = await client.start_new(function_name)
    logging.info(f"Started orchestration with ID = '{instance_id}'.")
    return client.create_check_status_response(req, instance_id)

# Orchestrator function
@app.orchestration_trigger(context_name="context")
def map_reduce_orchestrator(context: df.DurableOrchestrationContext):
    """
    Orchestrator function for the MapReduce workflow.
    """
    # Step 1: Get input data from Blob Storage
    input_files = yield context.call_activity("GetInputDataFn")

    # Step 2: Map phase
    map_tasks = [context.call_activity("MapperFn", file) for file in input_files]
    map_results = yield context.task_all(map_tasks)

    # Step 3: Shuffle phase
    shuffle_result = yield context.call_activity("ShufflerFn", map_results)

    # Step 4: Reduce phase
    reduce_result = yield context.call_activity("ReducerFn", shuffle_result)

    # Step 5: Return the final result
    return reduce_result

# Activity function to get input data from Blob Storage
@app.activity_trigger(input_name="input")
def get_input_data_fn(input: str) -> list:
    """
    Activity function to fetch input data from Azure Blob Storage.
    """
    import os
    from azure.storage.blob import BlobServiceClient

    # Get the connection string from environment variables
    connection_string = os.getenv("StorageConnectionString")

    # Connect to Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("input-files")

    # List all blobs in the container
    input_files = []
    for blob in container_client.list_blobs():
        blob_client = container_client.get_blob_client(blob.name)
        blob_data = blob_client.download_blob().readall().decode("utf-8")
        input_files.append(blob_data)

    return input_files

# Activity function for the Map phase
@app.activity_trigger(input_name="fileContent")
def mapper_fn(file_content: str) -> list:
    """
    Activity function for the Map phase.
    """
    # Tokenize the file content into words
    words = file_content.split()
    # Emit (word, 1) pairs
    return [(word.lower(), 1) for word in words]

# Activity function for the Shuffle phase
@app.activity_trigger(input_name="mapResults")
def shuffler_fn(map_results: list) -> dict:
    """
    Activity function for the Shuffle phase.
    """
    # Group (word, 1) pairs by word
    shuffle_result = defaultdict(list)
    for result in map_results:
        for word, count in result:
            shuffle_result[word].append(count)
    return shuffle_result

# Activity function for the Reduce phase
@app.activity_trigger(input_name="shuffleResult")
def reducer_fn(shuffle_result: dict) -> dict:
    """
    Activity function for the Reduce phase.
    """
    # Sum the counts for each word
    return {word: sum(counts) for word, counts in shuffle_result.items()}