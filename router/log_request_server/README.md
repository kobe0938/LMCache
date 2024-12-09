uvicorn main:app --host 0.0.0.0 --port 8080 --reload

docker build:
docker build --platform=linux/amd64 -t kobe0938/log-request-server:latest .

docker run:
docker run -p 8080:8080 kobe0938/log-request-server:latest

docker get the csv:
docker cp <container_id>:/app/requests.csv ./requests.csv
or
kubectl cp <namespace>/<pod_name>:/app/requests.csv ./requests.csv

dokcer push to hub:
docker push kobe0938/log-request-server:latest