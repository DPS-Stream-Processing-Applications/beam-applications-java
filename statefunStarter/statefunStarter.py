from kafka import KafkaConsumer, KafkaProducer
from kubernetes import client, config
import time
import yaml
from os import path


consumer = KafkaConsumer("topicA", bootstrap_servers=["localhost:9093"])
producer = KafkaProducer(
    bootstrap_servers=["localhost:9093"],
    key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
    value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
)


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
            print(
                f"Deployment '{deployment_name}' is ready and Service '{service_name}' is ready."
            )
            return True
        time.sleep(interval)
    print(
        f"Timeout reached. Deployment '{deployment_name}' or Service '{service_name}' not ready."
    )
    return False


def read_manifest(path_manifest):
    with open(path_manifest, "r") as f:
        return list(yaml.safe_load_all(f))


def start_deployment_and_service(message, path_manifest):
    print("Starting deployment and service")
    config.load_kube_config()
    k8s_core_v1 = client.CoreV1Api()
    k8s_apps_v1 = client.AppsV1Api()
    deployment_name = None
    service_name = None
    for doc in path_manifest:
        kind = doc.get("kind")
        metadata = doc.get("metadata", {})
        name = metadata.get("name")
        if kind == "Deployment":
            deployment_name = name
            resp = k8s_apps_v1.create_namespaced_deployment(
                body=doc, namespace="statefun"
            )
            print(
                f"Deployment '{deployment_name}' created. Status='{resp.metadata.name}'"
            )
        elif kind == "Service":
            service_name = name
            resp = k8s_core_v1.create_namespaced_service(body=doc, namespace="statefun")
            print(f"Service '{service_name}' created. Status='{resp.metadata.name}'")

    if deployment_name and service_name:
        if wait_for_deployment_and_service(
            k8s_apps_v1, k8s_core_v1, deployment_name, service_name
        ):
            producer.send(
                "senml-source",
                key=str(int(time.time() * 1000)).encode("utf-8"),
                value=message.decode(),
            )
        else:
            print("Deployment or Service did not become ready in time.")
    else:
        print("Deployment or Service name not found in manifest.")


def terminate_deployment_and_service(manifest_docs):
    config.load_kube_config()
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
            print(f"Deployment '{name}' deleted")
        elif kind == "Service":
            resp = k8s_core_v1.delete_namespaced_service(
                name=name, namespace="statefun"
            )
            print(f"Service '{name}' deleted")


def main(manifest_docs):
    last_message_time = time.time()
    is_deployed = False
    while True:
        for message in consumer:
            start_deployment_and_service(message.value, manifest_docs)
            time.sleep(60)
            terminate_deployment_and_service(manifest_docs)


if __name__ == "__main__":
    try:
        # FIXME: pass path as argument
        path_manifest = "/home/jona/Documents/Bachelor_thesis/repos/official_repo/beam-applications-java/statefunApplication/k8s/03-train-functions/functions-service.yaml"
        manifest_docs = read_manifest(path_manifest)
        main(manifest_docs)
    except KeyboardInterrupt:
        print("Shutting down")
    finally:
        consumer.close()
        producer.close()
        terminate_deployment_and_service(manifest_docs)
