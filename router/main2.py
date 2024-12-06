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
machine_request_counts = [0] * len(MACHINES)
machine_queues = [Queue() for _ in MACHINES]
machine_locks = [threading.Lock() for _ in MACHINES]
machine_timestamps = [[] for _ in MACHINES]  # Store request timestamps for each machine

# Throttling management
def reset_request_counts():
    while True:
        time.sleep(1)
        for i in range(len(machine_request_counts)):
            with machine_locks[i]:
                machine_request_counts[i] = 0

reset_thread = threading.Thread(target=reset_request_counts, daemon=True)
reset_thread.start()

# Status printer
def print_status():
    while True:
        time.sleep(10)
        print("Machine Status:")
        current_time = time.time()
        for i, (queue, count, timestamps) in enumerate(zip(machine_queues, machine_request_counts, machine_timestamps)):
            # Remove old timestamps outside the time window (10 seconds)
            machine_timestamps[i] = [t for t in timestamps if current_time - t <= 10]
            qps = len(machine_timestamps[i]) / 10  # Calculate QPS over the last 10 seconds
            print(f"Machine {i}: Queue Size = {queue.qsize()}, Requests in last second = {count}, QPS = {qps:.2f}")

status_thread = threading.Thread(target=print_status, daemon=True)
status_thread.start()

@app.post("/route")
async def route_request(request: Request):
    try:
        # Parse the incoming request
        body = await request.json()
        user_id = request.headers.get("user_id")
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id header is missing")

        # Determine the machine for this user
        if user_id not in user_to_machine:
            assigned_machine = min(range(len(MACHINES)), key=lambda i: machine_queues[i].qsize())
            user_to_machine[user_id] = assigned_machine
        else:
            assigned_machine = user_to_machine[user_id]

        machine_url = MACHINES[assigned_machine]

        print(f"Request: user_id={user_id} | machine={assigned_machine} | machine_url={MACHINES[assigned_machine]} | queue_size={machine_queues[assigned_machine].qsize()} | current_machine_request_counts={machine_request_counts[assigned_machine]} | timestamp={time.strftime('%H:%M:%S')}")

        # Throttle requests if QPS limit is reached
        def wait_for_slot():
            while True:
                with machine_locks[assigned_machine]:
                    if machine_request_counts[assigned_machine] < QPS_LIMIT:
                        machine_request_counts[assigned_machine] += 1
                        machine_timestamps[assigned_machine].append(time.time())
                        return
                time.sleep(0.1)

        wait_for_slot()

        # Forward the request to the assigned machine
        async with httpx.AsyncClient() as client:
            response = await client.post(machine_url, json=body, headers=request.headers)
        
        return response.json()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error routing request: {str(e)}")