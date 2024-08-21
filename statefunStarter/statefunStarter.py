from kafka import KafkaConsumer, KafkaProducer
from kubernetes import client, config
import time
import yaml
from os import path


consumer = KafkaConsumer("topicA", bootstrap_servers=["localhost:9093"])
producer = KafkaProducer(bootstrap_servers=["localhost:9093"])


def is_pod_running(k8s_core_v1, pod_name, namespace="default"):
    pod = k8s_core_v1.read_namespaced_pod(name=pod_name, namespace=namespace)
    return pod.status.phase == "Running"


def is_service_ready(k8s_core_v1, service_name, namespace="default"):
    endpoints = k8s_core_v1.read_namespaced_endpoints(
        name=service_name, namespace=namespace
    )
    return len(endpoints.subsets) > 0


def wait_for_pod_and_service(
    k8s_core_v1, pod_name, service_name, namespace="default", timeout=300, interval=5
):
    start_time = time.time()

    while time.time() - start_time < timeout:
        if is_pod_running(k8s_core_v1, pod_name, namespace) and is_service_ready(
            k8s_core_v1, service_name, namespace
        ):
            print(f"Pod '{pod_name}' is running and Service '{service_name}' is ready.")
            return True
        time.sleep(interval)

    print(f"Timeout reached. Pod '{pod_name}' or Service '{service_name}' not ready.")
    return False


def start_pods(message, path_manifest):
    print("started functions pod")
    config.load_kube_config()
    print(path_manifest)
    with open(path_manifest) as f:
        docs = yaml.safe_load_all(f)
        k8s_core_v1 = client.CoreV1Api()
        for doc in docs:
            kind = doc.get("kind")
            metadata = doc.get("metadata", {})
            name = metadata.get("name")

            if kind == "Pod":
                pod_name = name
                resp = k8s_core_v1.create_namespaced_pod(body=doc, namespace="default")
                print(f"Pod '{pod_name}' created. Status='{resp.metadata.name}'")
            elif kind == "Service":
                service_name = name
                resp = k8s_core_v1.create_namespaced_service(
                    body=doc, namespace="default"
                )
                print(
                    f"Service '{service_name}' created. Status='{resp.metadata.name}'"
                )

    if pod_name and service_name:
        if wait_for_pod_and_service(k8s_core_v1, pod_name, service_name):
            producer.send("topicB", value=message)
        else:
            print("Pod or Service did not become ready in time.")
    else:
        print("Pod or Service name not found in manifest.")


def terminate_pods(path_manifest):
    config.load_kube_config()
    print(path_manifest)

    with open(path_manifest) as f:
        docs = yaml.safe_load_all(f)
        k8s_core_v1 = client.CoreV1Api()
        for doc in docs:
            kind = doc.get("kind")
            metadata = doc.get("metadata", {})
            name = metadata.get("name")

            if kind == "Pod":
                resp = k8s_core_v1.delete_namespaced_pod(name=name, namespace="default")
                print(f"Pod '{name}' deleted")
            elif kind == "Service":
                resp = k8s_core_v1.delete_namespaced_service(
                    name=name, namespace="default"
                )
                print(f"Service '{name}' deleted")


def main():
    path_manifest = path.join(path.dirname(__file__), "nginx.yaml")
    while True:
        for message in consumer:
            start_pods(message.value, path_manifest)
            time.sleep(60)
            terminate_pods(path_manifest)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Shutting down")
    finally:
        consumer.close()
        producer.close()
