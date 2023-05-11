from prometheus_api_client import PrometheusConnect
import time
from kubernetes import client, config
import kubernetes.client
from pint import UnitRegistry
from collections import defaultdict
timeout_seconds = 30
resources = {}
clusters=[]

ureg = UnitRegistry()
Q_ = ureg.Quantity
# Memory units
ureg.define('kmemunits = 1 = [kmemunits]')
ureg.define('Ki = 1024 * kmemunits')
ureg.define('Mi = Ki^2')
ureg.define('Gi = Ki^3')
ureg.define('Ti = Ki^4')
ureg.define('Pi = Ki^5')
ureg.define('Ei = Ki^6')

# cpu units
ureg.define('kcpuunits = 1 = [kcpuunits]')
ureg.define('m = 1/1000 * kcpuunits')
ureg.define('k = 1000 * kcpuunits')
ureg.define('M = k^2')
ureg.define('G = k^3')
ureg.define('T = k^4')
ureg.define('P = k^5')
ureg.define('E = k^6')

def get_all_federation_clusters():
    config.load_kube_config()

    api_instance = client.CustomObjectsApi()

    group = 'core.kubefed.io'  # str | The custom resource's group name
    version = 'v1beta1'  # str | The custom resource's version
    namespace = 'kube-federation-system'  # str | The custom resource's namespace
    plural = 'kubefedclusters'  # str | The custom resource's plural name. For TPRs this would be lowercase plural kind.
    pretty = 'true'

    clusters = []

    try:
        api_response = api_instance.list_namespaced_custom_object(group, version, namespace, plural, pretty=pretty, _request_timeout=timeout_seconds)
        for item in api_response['items']:
            clusters.append(item['metadata']['name'])
    except:
        print("Connection timeout after " + str(timeout_seconds) + " seconds to host cluster")

    return clusters


def getControllerMasterIP(cluster):
    config.load_kube_config()
    api_instance = client.CoreV1Api(api_client=config.new_client_from_config(context=cluster))
    #api_instance = kubernetes.client.CoreV1Api()
    master_ip = ""
    try:
        nodes = api_instance.list_node(pretty=True, _request_timeout=timeout_seconds)
        nodes = [node for node in nodes.items if
                 'node-role.kubernetes.io/master' in node.metadata.labels]
        # get all addresses of the master
        addresses = nodes[0].status.addresses

        master_ip = [i.address for i in addresses if i.type == "InternalIP"][0]
    except:
        print("Connection timeout after " + str(timeout_seconds) + " seconds to host cluster")

    return master_ip

def getControllerMasterIPHere():
    config.load_kube_config()
    #api_instance = client.CoreV1Api(api_client=config.new_client_from_config(context=cluster))
    api_instance = kubernetes.client.CoreV1Api()
    master_ip = ""
    try:
        nodes = api_instance.list_node(pretty=True, _request_timeout=timeout_seconds)
        nodes = [node for node in nodes.items if
                 'node-role.kubernetes.io/master' in node.metadata.labels]
        # get all addresses of the master
        addresses = nodes[0].status.addresses

        master_ip = [i.address for i in addresses if i.type == "InternalIP"][0]
    except:
        print("Connection timeout after " + str(timeout_seconds) + " seconds to host cluster")

    return master_ip

def getresources(mode,cluster):
    start = time.perf_counter()
    total=0
    cp=getControllerMasterIP(cluster)
    print(cp)
    prom_host = getControllerMasterIPHere()
    prom_port = 30090
    prom_url = "http://" + str(prom_host) + ":" + str(prom_port)
    pc = PrometheusConnect(url=prom_url, disable_ssl=True)
    if mode == "CPU" or mode == 'cpu':
        query="(sum(increase(node_cpu_seconds_total{cluster_name=\"" + cluster + "\",mode=\"idle\"}[30s]))by (instance)/sum(increase(node_cpu_seconds_total{cluster_name=\"" + cluster + "\"}[30s]))by (instance))*100"
        print(query)
        result = pc.custom_query(query=query)
        if len(result) > 0:
            for node in result:
                ip=str(node['metric']['instance']).split(":")
                if ip[0]!=cp:
                    total+=float((node['value'][1]))
                    print(node)
                    print(float((node['value'][1])))
                    print(total)
            print(total)

    else:
        print("Please input cpu or Memory")
    end = time.perf_counter()

#clusters=get_all_federation_clusters()
#for cluster in clusters:
#    print(getControllerMasterIP(cluster))

def compute_available_resources(cluster):

    total_allocatable_cpu = 0
    total_allocatable_memory = 0

    available_cpu = 0
    available_memory = 0

    total_cpu_request = 0
    total_memory_request = 0

    core_v1 = client.CoreV1Api(api_client=config.new_client_from_config(context=cluster))

    available_resources_per_node = []

    try:
        for node in core_v1.list_node(_request_timeout=timeout_seconds).items[1:]:
            stats          = {}
            node_name      = node.metadata.name
            allocatable    = node.status.allocatable
            allocatabale_cpu = Q_(allocatable['cpu']).to('m')
            allocatable_memory = Q_(allocatable['memory'])
            total_allocatable_cpu += allocatabale_cpu
            total_allocatable_memory += allocatable_memory
            max_pods       = int(int(allocatable["pods"]) * 1.5)
            field_selector = ("status.phase!=Succeeded,status.phase!=Failed," +
                              "spec.nodeName=" + node_name)

            node_cpu_request = 0
            node_memory_request = 0

            pods = core_v1.list_pod_for_all_namespaces(limit=max_pods,
                                                       field_selector=field_selector).items
            cpureqs, memreqs = [], []
            for pod in pods:
                for container in pod.spec.containers:
                    res = container.resources
                    reqs = defaultdict(lambda: 0, res.requests or {})
                    cpureqs.append(Q_(reqs["cpu"]))
                    memreqs.append(Q_(reqs["memory"]))

            node_cpu_request += sum(cpureqs)
            node_memory_request += sum(memreqs)

            dict = {}

            dict['name'] = node_name
            dict['cpu'] = float(allocatabale_cpu - node_cpu_request) * 1000
            dict['memory'] = float(allocatable_memory - node_memory_request) / (1024 * 1024)

            available_resources_per_node.append(dict)

            total_cpu_request += Q_(node_cpu_request)
            total_memory_request += Q_(node_memory_request).to('Ki')
        available_cpu = total_allocatable_cpu - total_cpu_request
        available_memory = total_allocatable_memory - total_memory_request

        available_cpu = float(str(available_cpu)[:-2])
        available_memory = float(str(available_memory)[:-3])
    except:
        print("Connection timeout after " + str(timeout_seconds) + " seconds on cluster " + cluster)
    return available_cpu, available_memory, available_resources_per_node

#print(compute_available_resources("cluster1"))
getresources("CPU","cluster2")