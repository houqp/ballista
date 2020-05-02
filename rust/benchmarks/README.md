# Ballista Rust Benchmarks

## Local Mode

To run for a single month (single thread).

```bash
cargo run --release -- --mode local --path /mnt/nyctaxi/csv/2019/yellow_tripdata_2019-01.csv
```

To run for multiple files (one thread per file).
```bash
cargo run --release -- --mode local --path /mnt/nyctaxi/csv/2019
```

## Kubernetes

*NOTE: this is a work-in-progress and is not functional yet*

This example shows how to manually create a Ballista cluster of Rust executors and run an aggregate query in parallel across those executors.

## Prerequisites

You will need to create a Ballista cluster in Kubernetes. This is documented in the [README](../../../kubernetes/README.md) in the top-level kubernetes folder. 

## Build Example Docker Image

If you are using Minikube, make sure your docker environment is pointing to the Docker daemon running in Minikube.

```bash
eval $(minikube -p minikube docker-env)
```

From this directory.

```bash
./build-docker-image.sh
```

## Deploy Example

Run the following kubectl command to deploy the example as a job.

```bash
kubectl apply -f parallel-aggregate-rs.yaml
```

Run the `kubectl get pods` again to find the pod for the example.

```bash
kubectl get pods
NAME                          READY   STATUS      RESTARTS   AGE
ballista-0                    1/1     Running     0          105m
ballista-1                    1/1     Running     0          105m
parallel-aggregate-rs-5vjj2   0/1     Completed   0          95m
```

Run the `kubectl logs` command to see the logs from the example.

```bash
kubectl logs parallel-aggregate-rs-5vjj2
```

## Teardown

Remove cluster:

```bash
kubectl delete -f parallel-aggregate-rs.yaml
kubectl delete -f cluster-deployment.yaml
```