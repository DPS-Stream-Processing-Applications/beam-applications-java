from kafka import KafkaConsumer, KafkaProducer
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
import time
import yaml
import os
import requests
import logging
import sys
import signal


consumer = KafkaConsumer(
    "statefun-starter-input",
    bootstrap_servers=["kafka-cluster-kafka-bootstrap.default.svc:9092"],
)
producer = KafkaProducer(
    bootstrap_servers=["kafka-cluster-kafka-bootstrap.default.svc:9092"],
    key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
    value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
)
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def handle_sigterm(signum, frame):
    logging.info("Received SIGTERM signal. Shutting down gracefully...")
    cleanup()
    exit(0)


def cleanup():
    try:
        consumer.close()
        producer.close()
        terminate_deployment_and_service(manifest_docs)
    except Exception as e:
        logging.info(f"Cleanup error: {e}")
        logging.info("functions pod was already down")


def check_if_service_is_already_running(name):
    k8s_core_v1 = client.CoreV1Api()
    try:
        k8s_core_v1.read_namespaced_service(name=name, namespace="statefun")
        logging.info(f"Service '{name}' already exists. Skipping creation.")
    except ApiException as e:
        if e.status == 404:
            logging.info(f"Service '{name}'. is new")


def read_metric_from_prometheus(metric_name):
    prometheus_url = "http://localhost:9090"
    try:
        response = requests.get(
            f"{prometheus_url}/api/v1/query",
            params={"query": metric_name},
        )
        data = response.json()
        value = data["data"]["result"][0]["value"][1]
        return int(value)
    except Exception as e:
        logging.error(f"Error, when reading from prometheus: {e}")
        return 0


def check_if_pod_is_idle(metric_name, num_records_in, application):
    if application == "PRED":
        return 2 * num_records_in <= read_metric_from_prometheus(metric_name)

    if application == "TRAIN":
        return 2 * num_records_in <= read_metric_from_prometheus(metric_name)


def is_pod_running(k8s_core_v1, pod_name, namespace="statefun"):
    pod = k8s_core_v1.read_namespaced_pod(name=pod_name, namespace=namespace)
    return pod.status.phase == "Running"


def is_deployment_ready(k8s_apps_v1, deployment_name, namespace="statefun"):
    deployment = k8s_apps_v1.read_namespaced_deployment(
        name=deployment_name, namespace=namespace
    )
    return deployment.status.ready_replicas == deployment.spec.replicas


def is_service_ready(k8s_core_v1, service_name, namespace="statefun"):
    endpoints = k8s_core_v1.read_namespaced_endpoints(
        name=service_name, namespace=namespace
    )
    return len(endpoints.subsets) > 0


def wait_for_deployment_and_service(
    k8s_apps_v1,
    k8s_core_v1,
    deployment_name,
    service_name,
    namespace="statefun",
    timeout=300,
    interval=5,
):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if is_deployment_ready(
            k8s_apps_v1, deployment_name, namespace
        ) and is_service_ready(k8s_core_v1, service_name, namespace):
            logging.info(
                f"Deployment '{deployment_name}' is ready and Service '{service_name}' is ready."
            )
            return True
        time.sleep(interval)
    logging.error(
        f"Timeout reached. Deployment '{deployment_name}' or Service '{service_name}' not ready."
    )
    return False


def read_manifest(path_manifest, mongodb_address, dataset):
    with open(path_manifest, "r") as f:
        manifest = list(yaml.safe_load_all(f))
    for item in manifest:
        if item["kind"] == "Deployment":
            containers = item["spec"]["template"]["spec"]["containers"]
            for container in containers:
                env_vars = container.get("env", [])
                for env in env_vars:
                    if env["name"] == "MONGO_DB_ADDRESS":
                        env["value"] = mongodb_address
                    elif env["name"] == "DATASET":
                        env["value"] = dataset
    return manifest


def start_deployment_and_service(message, path_manifest):
    logging.info("Starting deployment and service")
    config.load_incluster_config()
    k8s_core_v1 = client.CoreV1Api()
    k8s_apps_v1 = client.AppsV1Api()
    deployment_name = None
    service_name = None
    start_time = time.time()
    for doc in path_manifest:
        kind = doc.get("kind")
        metadata = doc.get("metadata", {})
        name = metadata.get("name")
        if kind == "Deployment":
            deployment_name = name
            resp = k8s_apps_v1.create_namespaced_deployment(
                body=doc, namespace="statefun"
            )
            logging.info(
                f"Deployment '{deployment_name}' created. Status='{resp.metadata.name}'"
            )
        elif kind == "Service":
            service_name = name
            resp = k8s_core_v1.create_namespaced_service(
                body=doc, namespace="statefun", pretty="true"
            )
            logging.info(
                f"Service '{service_name}' created. Status='{resp.metadata.name}'"
            )

    if deployment_name and service_name:
        if wait_for_deployment_and_service(
            k8s_apps_v1, k8s_core_v1, deployment_name, service_name
        ):
            producer.send(
                "senml-source",
                key=str(int(time.time() * 1000)).encode("utf-8"),
                value=message.decode(),
            )
            end_time = time.time()
            duration = end_time - start_time
            logging.info(f"Time taken to create deployment: {duration:.2f} seconds")

        else:
            logging.error("Deployment or Service did not become ready in time.")
    else:
        logging.error("Deployment or Service name not found in manifest.")


def terminate_deployment_and_service(manifest_docs):
    config.load_incluster_config()
    k8s_core_v1 = client.CoreV1Api()
    k8s_apps_v1 = client.AppsV1Api()
    for doc in manifest_docs:
        kind = doc.get("kind")
        metadata = doc.get("metadata", {})
        name = metadata.get("name")
        if kind == "Deployment":
            resp = k8s_apps_v1.delete_namespaced_deployment(
                name=name, namespace="statefun"
            )
            logging.info(f"Deployment '{name}' deleted")
        elif kind == "Service":
            resp = k8s_core_v1.delete_namespaced_service(
                name=name, namespace="statefun"
            )
            logging.info(f"Service '{name}' deleted")


def main(manifest_docs, metric_name, application):
    is_deployed = False
    inactivity_timeout = 60
    number_sent_messages = 0
    while True:
        message = consumer.poll(timeout_ms=5000)
        if message:
            for tp, messages in message.items():
                for msg in messages:
                    if not is_deployed:
                        start_deployment_and_service(msg.value, manifest_docs)
                        is_deployed = True
                    else:
                        producer.send(
                            "senml-source",
                            key=str(int(time.time() * 1000)).encode("utf-8"),
                            value=msg.value.decode(),
                        )
                    last_message_time = time.time()
                    number_sent_messages = number_sent_messages + 1
        if (
            is_deployed
            and (time.time() - last_message_time > inactivity_timeout)
            and check_if_pod_is_idle(metric_name, number_sent_messages, application)
        ):
            terminate_deployment_and_service(manifest_docs)
            is_deployed = False


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        application = os.getenv("APPLICATION")
        mongodb_address = os.getenv("MONGODB")
        dataset = os.getenv("DATASET")
        if not (dataset == "FIT" or dataset == "SYS" or dataset == "TAXI"):
            raise Exception("Unsupported dataset argument")
        metric_name = None
        if application == "TRAIN":
            metric_name = "flink_taskmanager_job_task_operator_functions_pred_mqttPublishTrain_outEgress"
            path_manifest = "/app/train/functions-service.yaml"
        if application == "PRED":
            metric_name = "flink_taskmanager_job_task_operator_functions_pred_mqttPublish_outEgress"
            path_manifest = "/app/pred/functions-service.yaml"
        manifest_docs = read_manifest(path_manifest, mongodb_address, dataset)
        main(manifest_docs, metric_name, application)
    except KeyboardInterrupt:
        logging.info("Shutting down")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

    finally:
        consumer.close()
        producer.close()
        try:
            terminate_deployment_and_service(manifest_docs)
        except:
            logging.info("functions pod was already down")
