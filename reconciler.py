
import time
import threading
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

GROUP = "cosc3657.tu.ca"
VERSION = "v1alpha1"
PLURAL = "resilientdeployments"
NAMESPACE = "default"

config.load_kube_config()

core_api = client.CoreV1Api()
custom_api = client.CustomObjectsApi()
lock = threading.Lock()


def reconcile(resource):
    name = resource["metadata"]["name"]
    spec = resource["spec"]
    desired = int(spec["replicas"])
    image = spec["image"]

    selector = f"app.kubernetes.io/managed-by={name}"

    with lock:
        response = core_api.list_namespaced_pod(
            namespace=NAMESPACE,
            label_selector=selector
        )

        pods = [
            pod for pod in response.items
            if pod.metadata.deletion_timestamp is None
            and pod.status.phase not in ("Failed", "Succeeded")
        ]

        observed = len(pods)
        drift = desired - observed

        print(
            f"[RECONCILE LOOP] Resource: {name} | "
            f"Desired: {desired} | Observed: {observed} | "
            f"Drift: {drift}",
            flush=True
        )

        if drift > 0:
            print(f"Scale-Up Event: Provisioning {drift} pod(s)...")

            for _ in range(drift):
                pod = client.V1Pod(
                    metadata=client.V1ObjectMeta(
                        generate_name=f"{name}-",
                        namespace=NAMESPACE,
                        labels={
                            "app.kubernetes.io/managed-by": name
                        },
                        owner_references=[
                            client.V1OwnerReference(
                                api_version=f"{GROUP}/{VERSION}",
                                kind="ResilientDeployment",
                                name=name,
                                uid=resource["metadata"]["uid"],
                                controller=True
                            )
                        ]
                    ),
                    spec=client.V1PodSpec(
                        containers=[
                            client.V1Container(
                                name="echo",
                                image=image,
                                args=["-text=Hello from COSC 3657"]
                            )
                        ],
                        restart_policy="Always"
                    )
                )

                created = core_api.create_namespaced_pod(
                    namespace=NAMESPACE,
                    body=pod
                )
                print(f"Created pod: {created.metadata.name}")

        elif drift < 0:
            print(f"Scale-Down Event: Removing {-drift} pod(s)...")

            for pod in pods[desired:]:
                core_api.delete_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=NAMESPACE
                )
                print(f"Deleted pod: {pod.metadata.name}")

        else:
            print(
                f"EQUILIBRIUM REACHED: System state matches "
                f"target spec for '{name}'."
            )


def reconcile_all():
    resources = custom_api.list_namespaced_custom_object(
        group=GROUP,
        version=VERSION,
        namespace=NAMESPACE,
        plural=PLURAL
    )

    for resource in resources.get("items", []):
        try:
            reconcile(resource)
        except ApiException as error:
            print(f"Reconciliation error: {error}")


def watch_resources():
    while True:
        try:
            watcher = watch.Watch()

            for event in watcher.stream(
                custom_api.list_namespaced_custom_object,
                group=GROUP,
                version=VERSION,
                namespace=NAMESPACE,
                plural=PLURAL,
                timeout_seconds=30
            ):
                print(f"Resource event: {event['type']}")
                reconcile_all()

        except Exception as error:
            print(f"Resource watch error: {error}")
            time.sleep(2)


def watch_pods():
    while True:
        try:
            watcher = watch.Watch()

            for event in watcher.stream(
                core_api.list_namespaced_pod,
                namespace=NAMESPACE,
                timeout_seconds=30
            ):
                pod = event["object"]
                labels = pod.metadata.labels or {}

                if "app.kubernetes.io/managed-by" in labels:
                    print(f"Pod event: {event['type']}")
                    reconcile_all()

        except Exception as error:
            print(f"Pod watch error: {error}")
            time.sleep(2)


def main():
    print(
        "=== COSC 3657 Custom Kubernetes Reconciler Started ===",
        flush=True
    )

    threading.Thread(
        target=watch_resources,
        daemon=True
    ).start()

    threading.Thread(
        target=watch_pods,
        daemon=True
    ).start()

    while True:
        try:
            reconcile_all()
        except Exception as error:
            print(f"Controller error: {error}")

        time.sleep(2)


if __name__ == "__main__":
    main()
# Put your code here if python is chosen for this project.