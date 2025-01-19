import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    # Step 1: Get input data from Blob Storage
    input_files = yield context.call_activity("GetInputDataFn", None)

    # Step 2: Map phase
    map_tasks = [context.call_activity("MapperFn", file) for file in input_files]
    map_results = yield context.task_all(map_tasks)

    # Step 3: Shuffle phase
    shuffle_result = yield context.call_activity("ShufflerFn", map_results)

    # Step 4: Reduce phase
    reduce_tasks = [context.call_activity("ReducerFn", shuffle_result)]
    reduce_results = yield context.task_all(reduce_tasks)

    # Step 5: Return the final result
    return reduce_results

main = df.Orchestrator.create(orchestrator_function)