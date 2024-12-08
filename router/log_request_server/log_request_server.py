"""
A FastAPI server that provides transparent request logging functionality.
This server logs all incoming HTTP requests, including their method, URL, headers, 
and body content, as well as the response status code. It serves as a debugging
and monitoring tool to truthfully reflect all request information received.

Logs are written both to 'requests.log' file and to stdout.
"""

from fastapi import FastAPI, Request
import uvicorn
import csv
import os
from datetime import datetime

app = FastAPI()

# Global counter for requests
request_count = 0

# Prepare CSV logging
LOG_FILE = "requests.csv"
file_exists = os.path.isfile(LOG_FILE)
with open(LOG_FILE, 'a', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    # Write header if file didn't exist before
    if not file_exists:
        writer.writerow(["timestamp", "method", "url", "headers", "body"])


@app.middleware("http")
async def log_requests(request: Request, call_next):
    global request_count
    request_count += 1

    # Extract request data
    method = request.method
    url = str(request.url)
    headers = dict(request.headers)
    body_bytes = await request.body()
    body = body_bytes.decode('utf-8') if body_bytes else 'No body'
    timestamp = datetime.utcnow().isoformat()

    # Write to CSV
    with open(LOG_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, method, url, headers, body])

    # Print to stdout every 10 requests
    if request_count % 10 == 0:
        print(
            f"""
            [Request #{request_count}]
            Timestamp: {timestamp}
            Method: {method}
            URL: {url}
            Headers: {headers}
            Body: {body}
            -------------------""")

    # No response logging as requested
    response = await call_next(request)
    return response


@app.get("/")
async def read_root():
    return {"message": "Welcome to the logging server!"}


@app.post("/v1/chat/completions")
async def chat_completions(data: dict):
    return {"status": "completed", "data_received": data}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
