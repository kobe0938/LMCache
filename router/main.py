from collections import deque
import time
import threading
from queue import Queue
from fastapi import FastAPI, HTTPException, Request
from typing import Dict, Any
import httpx
from config import MACHINES, QPS_LIMIT

app = FastAPI()

# State management
user_to_machine: Dict[str, int] = {}  # Maps user_id to machine index
machine_queues = Queue()  # Global queue for all incoming requests
machine_request_timestamps = [deque() for _ in MACHINES]  # Track request timestamps per machine
machine_locks = [threading.Lock() for _ in MACHINES]

# Time window for QPS calculation (e.g., 10 seconds)
TIME_WINDOW = 10

# Status printer
def print_status():
    while True:
        time.sleep(10)
        print("Machine Status:")
        for i, timestamps in enumerate(machine_request_timestamps):
            current_time = time.time()
            # Remove old timestamps outside the time window
            with machine_locks[i]:
                while timestamps and current_time - timestamps[0] > TIME_WINDOW:
                    timestamps.popleft()
                qps = len(timestamps) / TIME_WINDOW
            print(f"Machine {i}: QPS = {qps:.2f}, Queue Size = {machine_queues.qsize()}")

status_thread = threading.Thread(target=print_status, daemon=True)
status_thread.start()

# Worker thread for processing requests
def process_requests():
    while True:
        user_id, body, headers = machine_queues.get()  # Dequeue a request

        # Determine the machine for this user
        if user_id in user_to_machine:
            assigned_machine = user_to_machine[user_id]
        else:
            # Assign to the first available machine
            assigned_machine = min(range(len(MACHINES)), key=lambda i: len(machine_request_timestamps[i]))
            user_to_machine[user_id] = assigned_machine

        machine_url = MACHINES[assigned_machine]

        # Wait for QPS slot
        while True:
            with machine_locks[assigned_machine]:
                current_time = time.time()
                # Remove old timestamps outside the time window
                while machine_request_timestamps[assigned_machine] and \
                      current_time - machine_request_timestamps[assigned_machine][0] > TIME_WINDOW:
                    machine_request_timestamps[assigned_machine].popleft()

                # Check QPS
                if len(machine_request_timestamps[assigned_machine]) < QPS_LIMIT * TIME_WINDOW:
                    machine_request_timestamps[assigned_machine].append(current_time)
                    break
            time.sleep(0.1)

        # Forward the request to the assigned machine
        try:
            with httpx.Client() as client:  # Use synchronous client
                response = client.post(machine_url, json=body, headers=headers)
                print(f"Request for user {user_id} processed by machine {assigned_machine}: {response.status_code}")
        except Exception as e:
            print(f"Error processing request for user {user_id} on machine {assigned_machine}: {e}")

# Start worker threads for processing requests
num_workers = len(MACHINES)  # One worker per machine
for _ in range(num_workers):
    threading.Thread(target=process_requests, daemon=True).start()

@app.post("/route")
async def route_request(request: Request):
    try:
        # Parse the incoming request
        body = await request.json()
        user_id = request.headers.get("user_id")
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id header is missing")

        # Add the request to the global queue
        machine_queues.put((user_id, body, dict(request.headers)))
        return {"status": "queued"}

    except Exception as e:
        import traceback
        raise HTTPException(status_code=500, detail=f"Error routing request: {str(e)}, Traceback: {traceback.format_exc()}")
