from camunda.external_task.external_task import ExternalTask, TaskResult
from camunda.external_task.external_task_worker import ExternalTaskWorker
from fastapi import FastAPI
import logging
import threading
from externalParallel import main

app = FastAPI()

@app.on_event("startup")
def on_startup():
    """
    Starts the Camunda worker in a background thread when the FastAPI app starts.
    """
    logging.info("Starting Camunda External Task Worker in background...")
    worker_thread = threading.Thread(target=start_worker, daemon=True)
    worker_thread.start()
    logging.info("FastAPI application started.")

# Configure the Camunda engine's REST API endpoint
CAMUNDA_URL = "http://localhost:8080/engine-rest" 
TOPIC_NAME = "process-weather"

# configuration for the Client
default_config = {
    "maxTasks": 1,
    "lockDuration": 10000,
    "asyncResponseTimeout": 5000,
    "retries": 3,
    "retryTimeout": 5000,
    "sleepSeconds": 30
}


def handle_weather_task(task: ExternalTask) -> TaskResult:
    """
    This function contains your business logic.
    """
    # Access process variables
    city = task.get_variable("city")

    # Add your business logic here (e.g., call a weather API)
    weather_status = f"Sunny in {city}" 
    all_variables = task.get_variables()
    print(f"All variables: {all_variables}")

    # Handle a BPMN Failure
    if city == "Mumbai":
            print("Mumbai Failed")
            task.bpmn_error(error_code="BPMN_ERROR", error_message="BPMN error occurred")

     # Handle task Failure
    elif city == "Chennai":
           print("Chennai Failed")
           task.failure(error_message="task failed",  error_details="failed task details", 
                        max_retries=3, retry_timeout=5000)
    # This client/worker uses max_retries if no retries are previously set in the task
    # if retries are previously set then it just decrements that count by one before reporting failure to Camunda
    # when retries are zero, Camunda creates an incident which then manually needs to be looked into on Camunda Cockpit            


    
    # Return variables to the process engine upon completion
    return task.complete({"weatherStatus": weather_status, "anotherVariable": True})

# if __name__ == '__main__':
#     worker = ExternalTaskWorker(worker_id="1", base_url=CAMUNDA_URL, config=default_config )
#     worker.subscribe(TOPIC_NAME, handle_weather_task)
#     worker.run() # Starts the long polling loop

def start_worker():
    """
    Function to start the Camunda External Task Worker.
    """
    worker = ExternalTaskWorker(worker_id="1", base_url=CAMUNDA_URL, config=default_config )
    # Subscribe to the topic 'dqm-test' (replace with your topic name)
    try:
        worker.subscribe(TOPIC_NAME, handle_weather_task)
    except Exception as e:
        logging.error(f"Error starting Camunda worker: {e}")

if __name__ == "__main__":
    # This block allows running the worker independently for testing
    start_worker()
    main()
