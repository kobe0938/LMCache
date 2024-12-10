import time
import uuid
import threading
from collections import deque, defaultdict
from queue import Queue
from typing import Dict, Any
import httpx
from fastapi import FastAPI, HTTPException, Request, Response
import uvicorn

# Configuration
MACHINES = [
    "http://machine0:8000",
    "http://machine1:8000",
    "http://machine2:8000",
    "http://machine3:8000"
]
TIME_WINDOW = 10
TARGET_QPS = 1.8

app = FastAPI()

# Data structures
user_to_machine: Dict[str, int] = {}  # user_id -> machine_id
request_table: Dict[str, Dict[str, Any]] = {}  # request_id -> {user_id, body, arrival_time, allocated_machine_id, execution_time}

# For tracking requests on each machine
requests_on_each_machine = [deque() for _ in range(len(MACHINES))]  # store request_ids in order they arrive

# Queues for unprocessed requests per machine
queues_on_each_machine = [Queue() for _ in MACHINES]

# Locks
machine_locks = [threading.Lock() for _ in MACHINES]

# For statistics
# We'll keep track of total processed requests per machine to compute total QPS.
machine_processed_count = [0 for _ in MACHINES]
start_time = time.time()

# For user_id request count
user_request_count = defaultdict(int)  # user_id -> number of requests total

# Utility functions
def calculate_current_qps(machine_id: int) -> float:
    """Calculate current QPS for the machine by looking back at requests_on_each_machine."""
    # Current QPS: count how many requests were processed in the last TIME_WINDOW seconds
    # Since we mark execution_time when processed, we can look at that.
    current_time = time.time()
    count = 0
    with machine_locks[machine_id]:
        # We'll traverse from the end to find how many processed requests fall into the last TIME_WINDOW
        for req_id in reversed(requests_on_each_machine[machine_id]):
            exec_time = request_table[req_id].get("execution_time")
            if exec_time is None:
                # This means it's not processed yet, skip (these will appear at the end of the deque)
                continue
            if current_time - exec_time <= TIME_WINDOW:
                count += 1
            else:
                # Since requests_on_each_machine is chronological, once we find a request out of window, we can break
                break
    current_qps = count / TIME_WINDOW
    return current_qps

def calculate_total_qps(machine_id: int) -> float:
    """Calculate total QPS since start."""
    elapsed = time.time() - start_time
    return machine_processed_count[machine_id] / elapsed if elapsed > 0 else 0.0

def machine_worker(machine_id: int):
    """A thread worker that continuously checks queue and processes requests if QPS < TARGET_QPS."""
    while True:
        time.sleep(0.1)  # slightly sleep to prevent busy loop
        # Check QPS
        current_qps = calculate_current_qps(machine_id)
        if current_qps < TARGET_QPS:
            # If there's a request to process
            if not queues_on_each_machine[machine_id].empty():
                req_id = queues_on_each_machine[machine_id].get()
                # Process the request: forward it to the underlying machine
                process_request(machine_id, req_id)

def process_request(machine_id: int, req_id: str):
    """Forward the request to the machine and record execution time."""
    # Extract info from request_table
    user_id = request_table[req_id]["user_id"]
    body = request_table[req_id]["body"]
    machine_url = MACHINES[machine_id]

    # Forward the request to the machine
    # In a real scenario, handle headers and errors properly.
    # For demonstration, we just send a POST request with the body.
    headers = {"user_id": user_id}
    try:
        with httpx.Client() as client:
            response = client.post(machine_url, json=body, headers=headers)
            # Store execution time
            request_table[req_id]["execution_time"] = time.time()

            # Increase processed count
            with machine_locks[machine_id]:
                machine_processed_count[machine_id] += 1

    except Exception as e:
        # On error, we might want to log or handle differently
        print(f"Error processing request {req_id} on machine {machine_id}: {e}")

def print_status():
    """Background thread to print status periodically."""
    while True:
        time.sleep(10)
        print("=== Status Report ===")
        total_users_assigned_to_machine = [0]*len(MACHINES)
        # Count how many unique users each machine has
        inverse_map = defaultdict(list)
        for u, m in user_to_machine.items():
            inverse_map[m].append(u)
        for i in range(len(MACHINES)):
            total_users_assigned_to_machine[i] = len(inverse_map[i])

        for i in range(len(MACHINES)):
            qps = calculate_current_qps(i)
            total_q = queues_on_each_machine[i].qsize()
            total_qps = calculate_total_qps(i)
            print(f"Machine {i}: Current QPS={qps:.2f}, Total QPS={total_qps:.2f}, Queue Size={total_q}, Assigned Users={total_users_assigned_to_machine[i]}")

        # Overall stats: how many requests per user
        # This might be large, so maybe print a summary
        total_requests = sum(user_request_count.values())
        unique_users = len(user_request_count)
        print(f"Overall: Total Requests={total_requests}, Unique Users={unique_users}")
        # If needed, print user request distribution
        # For brevity, skip printing all users. But we could if needed.
        print("====================")

@app.post("/route")
async def route_request(request: Request):
    try:
        body = await request.json()
        user_id = request.headers.get("user_id")
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id header is missing")

        # Create a request_id
        req_id = str(uuid.uuid4())
        arrival_time = time.time()

        # Store in request_table
        request_table[req_id] = {
            "user_id": user_id,
            "body": body,
            "arrival_time": arrival_time,
            "allocated_machine_id": None,
            "execution_time": None
        }

        # Determine machine
        if user_id not in user_to_machine:
            # assign to machine with smallest queue
            machine_id = min(range(len(MACHINES)), key=lambda i: queues_on_each_machine[i].qsize())
            user_to_machine[user_id] = machine_id
        else:
            machine_id = user_to_machine[user_id]

        # Update request_table
        request_table[req_id]["allocated_machine_id"] = machine_id

        # Record the assignment
        with machine_locks[machine_id]:
            requests_on_each_machine[machine_id].append(req_id)
        
        # Add to machine queue
        queues_on_each_machine[machine_id].put(req_id)

        # Update user request count
        user_request_count[user_id] += 1

        # Immediately return a response indicating that request is queued/accepted.
        return {"status": "queued", "request_id": req_id, "assigned_machine": machine_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Start machine workers
for i in range(len(MACHINES)):
    t = threading.Thread(target=machine_worker, args=(i,), daemon=True)
    t.start()

# Start status printer thread
status_thread = threading.Thread(target=print_status, daemon=True)
status_thread.start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
