import asyncio
import time
from fastapi import FastAPI, BackgroundTasks

app = FastAPI()

# Store the status of the background task
task_status = {"completed": False}

# Function to perform the background task
def perform_background_task():
    # Simulate some work
    for i in range(1000000000000000000):
        print(i)
    task_status["completed"] = True

# Endpoint to trigger the background task
@app.post("/trigger_task/")
async def trigger_task(background_tasks: BackgroundTasks):
    # Add the background task to run in a separate thread
    # threading.Thread(target=perform_background_task).start()
    background_tasks.add_task(perform_background_task)
    return {"message": "Task triggered."}

# Endpoint to check the status of the background task
@app.get("/check_task_status/")
async def check_task_status():
    return {"completed": task_status["completed"]}
