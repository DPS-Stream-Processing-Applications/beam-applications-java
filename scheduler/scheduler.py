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
import subprocess
from kubernetes.stream import portforward


consumer = KafkaConsumer(
    "scheduler-input",
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
forwarder = None


def handle_sigterm(signum, frame):
    logging.info("Received SIGTERM signal. Shutting down scheduler gracefully...")
    cleanup()
    exit(0)


def cleanup():
    global forwarder
    try:
        consumer.close()
        producer.close()
        terminate_deployment(manifest_docs)
        stop_port_forward(forwarder)
    except Exception as e:
        logging.info(f"Cleanup error: {e}")
        logging.info("functions pod was already down")


def port_forward_service(service_name, namespace, local_port, service_port):
    try:
        config.load_incluster_config()
        v1 = client.CoreV1Api()

        service = v1.read_namespaced_service(name=service_name, namespace=namespace)

        selector = service.spec.selector
        if not selector:
            logging.error(f"Service {service_name} has no selector defined")
            return None

        pods = v1.list_namespaced_pod(
            namespace,
            label_selector=",".join([f"{k}={v}" for k, v in selector.items()]),
        )

        if not pods.items:
            logging.error(f"No pods found for service {service_name}")
            return None

        pod_name = pods.items[0].metadata.name
        logging.info(f"Forwarding from service '{service_name}' using pod '{pod_name}'")

        pf = portforward(
            v1.connect_get_namespaced_pod_portforward,
            pod_name,
            namespace,
            ports=str(local_port),
        )

        pf.local_port = local_port
        pf.remote_port = service_port
        logging.info(f"Port-forwarding started: {pf.local_port} -> {pf.remote_port}")

        return pf
    except Exception as e:
        logging.error(f"Port-forwarding error: {e}")
        return None


def stop_port_forward(pf):
    if pf:
        pf.close()
        logging.info("Port-forwarding stopped")


def read_metric_from_prometheus(metric_name):
    prometheus_url = "http://prometheus-operated.default.svc.cluster.local:9090"
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


def is_flink_deployment_ready(
    k8s_custom_objects_api, flink_deployment_name, namespace="default"
):
    flink_deployment = k8s_custom_objects_api.get_namespaced_custom_object(
        group="flink.apache.org",
        version="v1beta1",
        namespace=namespace,
        plural="flinkdeployments",
        name=flink_deployment_name,
    )

    job_manager_status = flink_deployment.get("status", {}).get(
        "jobManagerDeploymentStatus"
    )
    if job_manager_status == "READY":
        return True

    return False


def wait_for_flinkdeployment(
    k8s_apps_v1,
    deployment_name,
    namespace="default",
    timeout=300,
    interval=5,
):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if is_flink_deployment_ready(k8s_apps_v1, deployment_name, namespace):
            logging.info(f"Deployment '{deployment_name}' is ready.")
            return True
        time.sleep(interval)
    logging.error(f"Timeout reached. Deployment '{deployment_name} not ready.")
    return False


def read_manifest_serverful(path_manifest):
    with open(path_manifest, "r") as f:
        manifest = list(yaml.safe_load_all(f))
    return manifest


def start_deployment(path_manifest):
    logging.info("Starting flink-session-cluster")
    config.load_incluster_config()
    k8s_custom_objects_api = client.CustomObjectsApi()
    deployment_name = None
    start_time = time.time()
    for doc in path_manifest:
        kind = doc.get("kind")
        metadata = doc.get("metadata", {})
        name = metadata.get("name")
        if kind == "FlinkDeployment":
            deployment_name = name
            resp = k8s_custom_objects_api.create_namespaced_custom_object(
                group="flink.apache.org",
                version="v1beta1",
                plural="flinkdeployments",
                body=doc,
                namespace="default",
            )
            logging.info(
                f"FlinkDeployment '{deployment_name}' created. Status='{resp['metadata']['name']}'"
            )
    if deployment_name:
        if wait_for_flinkdeployment(k8s_custom_objects_api, deployment_name):
            end_time = time.time()
            duration = end_time - start_time
            logging.info(
                f"Time taken to create flink-session-cluster: {duration:.2f} seconds"
            )

        else:
            logging.error("Deployment did not become ready in time.")
    else:
        logging.error("Deployment name not found in manifest.")


def terminate_deployment(manifest_docs):
    config.load_incluster_config()
    k8s_custom_objects_api = client.CustomObjectsApi()

    for doc in manifest_docs:
        kind = doc.get("kind")
        metadata = doc.get("metadata", {})
        name = metadata.get("name")
        if kind == "FlinkDeployment":
            resp = k8s_custom_objects_api.delete_namespaced_custom_object(
                group="flink.apache.org",
                version="v1beta1",
                namespace="default",
                plural="flinkdeployments",
                name=name,
                body=client.V1DeleteOptions(),
            )
            logging.info(f"FlinkDeployment '{name}' deleted: {resp}")


def submit_flink_job(job_jar_path, job_manager_host, database_url, experiment_run_id):
    url = f"http://{job_manager_host}:8081/jars/upload"

    with open(job_jar_path, "rb") as jar_file:
        response = requests.post(url, files={"jarfile": jar_file})

    if response.status_code != 200:
        raise Exception(f"Failed to upload JAR file: {response.text}")

    jar_id = response.json()["filename"].split("/")[-1]

    logging.info("Jar: id" + str(jar_id))

    submit_url = f"http://{job_manager_host}:8081/jars/{jar_id}/run"
    job_params = {
        "programArgs": f"--databaseUrl={database_url} --experiRunId={experiment_run_id}"
    }

    response = requests.post(submit_url, json=job_params)
    logging.info(str(response.content))
    """
    if response.status_code != 200:
        logging.error("Failed jar-submission: "+str(response.status_code))
        raise Exception(f"Failed to submit job: {response.text}")

    """
    print(f"Job submitted successfully: {response.json()}")


def get_jobid_of_running_job(job_manager_host):
    submit_url = f"http://{job_manager_host}:8081/jobs"
    response = requests.get(submit_url)
    logging.info(str(response.content))
    if response.status_code == 200:
        jobs_data = response.json()
        running_jobs = []
        for job in jobs_data["jobs"]:
            if job["status"] == "RUNNING":
                running_jobs.append(job["id"])

        return running_jobs
    else:
        print(f"Failed to fetch jobs. Status code: {response.status_code}")
        return []


def stop_flink_job(job_manager_host, job_id):
    url = f"http://{job_manager_host}:8081/jobs/{job_id}/yarn-cancel"

    try:
        response = requests.get(url)

        if response.status_code == 200 or response.status_code == 202:
            print("Job stopped successfully.")
        else:
            print(f"Failed to stop the job. Status code: {response.status_code}")
            print(f"Response: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


def main(manifest_docs, application):
    is_deployed = False
    number_sent_messages = 0
    while True:
        message = consumer.poll(timeout_ms=5000)
        if message:
            for tp, messages in message.items():
                for msg in messages:
                    if not is_deployed:
                        start_deployment(manifest_docs)
                        is_deployed = True
                    last_message_time = time.time()
                    number_sent_messages = number_sent_messages + 1
                    submit_flink_job(
                        "/app/FlinkJob.jar",
                        "flink-session-cluster-rest",
                        os.getenv("MONGODB"),
                        application + "-120",
                    )
                    logging.info(f"Number of sent messages: {number_sent_messages}")


def debug_main(manifest_docs, application, dataset):
    """
    submit_flink_job(
        "/app/FlinkJob.jar",
        "flink-session-cluster-rest",
        os.getenv("MONGODB"),
        dataset + "-120",
    )
    """
    res = get_jobid_of_running_job("flink-session-cluster-rest")
    logging.info("Res: " + str(res))
    for job in res:
        stop_flink_job("flink-session-cluster-rest", job)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        application = os.getenv("APPLICATION")
        mongodb_address = os.getenv("MONGODB")
        dataset = os.getenv("DATASET")
        if not (dataset == "FIT" or dataset == "SYS" or dataset == "TAXI"):
            raise Exception("Unsupported dataset argument")
        path_manifest = "/app/flink-session-cluster-deployment.yaml"
        manifest_docs = read_manifest_serverful(path_manifest)
        # main(manifest_docs, application)
        debug_main(manifest_docs, application, dataset)
    except KeyboardInterrupt:
        logging.info("Shutting down")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

    finally:
        consumer.close()
        producer.close()
        try:
            pass
            # terminate_deployment(manifest_docs)
        except:
            logging.info("flink-session pod was already down")
