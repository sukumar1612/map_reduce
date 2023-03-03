FROM python:3.11-slim

WORKDIR /map_reduce

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "main.py" , "-worker"]

# change CMD option to -master to create master_node container

# Docker commands reference
# for master docker run --network map_reduce_network --network-alias master_node_config -p 5000:5000 master_node
# for worker docker run --network map_reduce_network -e MASTER_HOST_IP=master_node_config -e WORKER_HOST_IP=0.0.0.0 worker_node
# docker build --no-cache -t master_node -f Dockerfile .