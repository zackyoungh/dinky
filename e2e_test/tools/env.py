from typing import Optional

from requests import Session
import urllib.parse as urlparse
from hdfs.client import Client
import os
from config import *
from httpUtil import assertRespOk, url
from logger import log


def addStandaloneCluster(session: Session) -> int:
    """
    en: Add a cluster instance
    zh: 添加一个集群实例
    :param session:  requests.Session
    :return:  clusterId
    """
    name = 'flink-standalone'
    add_cluster_resp = session.put(url("api/cluster"), json={
        "name": name,
        "type": "standalone",
        "hosts": standalone_address
    })
    assertRespOk(add_cluster_resp, "Add cluster")
    get_data_list = session.get(url(f"api/cluster/list?searchKeyWord={urlparse.quote(name)}&isAutoCreate=false"))
    assertRespOk(get_data_list, "Get cluster list")
    for data in get_data_list.json()['data']:
        if data['name'] == name:
            return data['id']
    raise Exception(f"Cluster {name} not found")


def addApplicationCluster(session: Session, params: dict) -> Optional[int]:
    name = params['name']
    test_connection_yarn_resp = session.post(url("api/clusterConfiguration/testConnect"), json=params)
    assertRespOk(test_connection_yarn_resp, "Test yarn connectivity")
    test_connection_yarn_resp = session.put(url("api/clusterConfiguration/saveOrUpdate"), json=params)
    assertRespOk(test_connection_yarn_resp, "Add Yarn Application Cluster")
    get_app_list = session.get(url(f"api/clusterConfiguration/list?keyword={name}"), json=params)
    assertRespOk(get_app_list, "Get Yarn Application Cluster")
    for data in get_app_list.json()["data"]:
        if data["name"] == name:
            return data['id']


def addYarnCluster(session: Session) -> Optional[int]:
    client = Client("http://namenode:9870")
    flink_lib_path = yarn_flink_lib
    client.makedirs(flink_lib_path)
    # Traverse the specified path and upload the file to HDFS
    for root, dirs, files in os.walk(flink_lib_path):
        for file in files:
            filepath = os.path.join(root, file)
            client.upload(flink_lib_path + "/" + file, filepath)
    jar_path = yarn_dinky_app_jar
    client.makedirs(jar_path)
    for root, dirs, files in os.walk(jar_path):
        for file in files:
            if file.endswith(".jar") and file.__contains__("dinky-app"):
                filepath = os.path.join(root, file)
                jar_path = filepath
                client.upload(jar_path, filepath)
    name = "yarn-test"
    params = {
        "type": "yarn-application",
        "name": name,
        "enabled": True,
        "config": {
            "clusterConfig": {
                "hadoopConfigPath": yarn_hadoop_conf,
                "flinkLibPath": "hdfs://" + flink_lib_path,
                "flinkConfigPath": yarn_flink_conf
            },
            "flinkConfig": {
                "configuration": {
                    "jobmanager.memory.process.size": "1024m",
                    "taskmanager.memory.process.size": "1024m",
                    "taskmanager.numberOfTaskSlots": "1",
                    "state.savepoints.dir": "hdfs:///flink/savepoint",
                    "state.checkpoints.dir": "hdfs:///flink/ckp"
                }
            },
            "appConfig": {
                "userJarPath": "hdfs://" + jar_path
            }
        }
    }
    log.info(f"Adding yarn application cluster, parameters:{params}")
    return addApplicationCluster(session, params)


def addK8sNativeCluster(session: Session) -> Optional[int]:
    name = "k8s-native-test"
    params = {
        "type": "kubernetes-application",
        "name": name,
        "enabled": True,
        "config": {
            "kubernetesConfig": {
                "configuration": {
                    "kubernetes.rest-service.exposed.type": "NodePort",
                    "kubernetes.namespace": "dinky",
                    "kubernetes.service-account": "dinky",
                    "kubernetes.container.image": "registry.cn-hangzhou.aliyuncs.com/dinky-flink/dinky-flink:1.20.0-scala_2.12-java8"
                },
                "ingressConfig": {
                    "kubernetes.ingress.enabled": False
                },
                "kubeConfig": "---\r\napiVersion: v1\r\nclusters:\r\n- cluster:\r\n    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJkakNDQVIyZ0F3SUJBZ0lCQURBS0JnZ3Foa2pPUFFRREFqQWpNU0V3SHdZRFZRUUREQmhyTTNNdGMyVnkKZG1WeUxXTmhRREUzTXpRNU5EY3lOalF3SGhjTk1qUXhNakl6TURrME56UTBXaGNOTXpReE1qSXhNRGswTnpRMApXakFqTVNFd0h3WURWUVFEREJock0zTXRjMlZ5ZG1WeUxXTmhRREUzTXpRNU5EY3lOalF3V1RBVEJnY3Foa2pPClBRSUJCZ2dxaGtqT1BRTUJCd05DQUFUb3UvcWJSMlpJbnJiSWZtaVk5YTlJT2V3UXBpbEhJdWZvTi93NjlsSDEKdHZpbW16azJlM2hSSmtreloreFptMWRXN3l1M2FuaVZnN3Z3dkEvMVV0Ri9vMEl3UURBT0JnTlZIUThCQWY4RQpCQU1DQXFRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBZEJnTlZIUTRFRmdRVXM0dDVucW1oSmhKQW5XNEdBS3BJClBCVnk2dWN3Q2dZSUtvWkl6ajBFQXdJRFJ3QXdSQUlnV2tiZ3JybkFMdmxGTldiLzFzTkNISGJhYUgwUmIxS3MKMTY0dVN0TFdPOThDSUZMMHdvcEtJdUZIU25iRVVxNzdDMWFwVXJlNG1Kend5dy9tM0VtcGo0emkKLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=\r\n    server: https://127.0.0.1:37749\r\n  name: k3d-demo\r\ncontexts:\r\n- context:\r\n    cluster: k3d-demo\r\n    user: admin@k3d-demo\r\n  name: k3d-demo\r\ncurrent-context: k3d-demo\r\nkind: Config\r\npreferences: {}\r\nusers:\r\n- name: admin@k3d-demo\r\n  user:\r\n    client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJrRENDQVRlZ0F3SUJBZ0lJTzB3eG5RNG9oRmd3Q2dZSUtvWkl6ajBFQXdJd0l6RWhNQjhHQTFVRUF3d1kKYXpOekxXTnNhV1Z1ZEMxallVQXhOek0wT1RRM01qWTBNQjRYRFRJME1USXlNekE1TkRjME5Gb1hEVEkxTVRJeQpNekE1TkRjME5Gb3dNREVYTUJVR0ExVUVDaE1PYzNsemRHVnRPbTFoYzNSbGNuTXhGVEFUQmdOVkJBTVRESE41CmMzUmxiVHBoWkcxcGJqQlpNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQTBJQUJEbmM0SVZDTExRVzB2VmEKdUtLbGZYdFRRRElrSFl0dFVsQkFxaFpwd0Z6d3BvaUJQaW8xZzdsRG5ja0tVNFBzS0oreG5GSnpPd0V0ZlFaaAprakZkOC9XalNEQkdNQTRHQTFVZER3RUIvd1FFQXdJRm9EQVRCZ05WSFNVRUREQUtCZ2dyQmdFRkJRY0RBakFmCkJnTlZIU01FR0RBV2dCVDZSVTBOL3k3VUU0WEMxTmpCQ2xNajhsZDNzekFLQmdncWhrak9QUVFEQWdOSEFEQkUKQWlBZGpDMWU1ODBUMjZiSDluSXZoaE1pOGZoYnRQT2hzMnJ1OVV6MGZ4eExBd0lnZUNYcWNGdnFMbGs0KzJDSwpndFVwZ0txWVkxeDhEck9LZWNRMGdrUzV2QVk9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0KLS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJkekNDQVIyZ0F3SUJBZ0lCQURBS0JnZ3Foa2pPUFFRREFqQWpNU0V3SHdZRFZRUUREQmhyTTNNdFkyeHAKWlc1MExXTmhRREUzTXpRNU5EY3lOalF3SGhjTk1qUXhNakl6TURrME56UTBXaGNOTXpReE1qSXhNRGswTnpRMApXakFqTVNFd0h3WURWUVFEREJock0zTXRZMnhwWlc1MExXTmhRREUzTXpRNU5EY3lOalF3V1RBVEJnY3Foa2pPClBRSUJCZ2dxaGtqT1BRTUJCd05DQUFTYnlzbDJ2NFp6YTBna0s4cWJIU3MvUS8zQTE5US9Mek1jazM4OW5zZkYKQjExMzlPM29ITWlUTS9weDVsa09wcHAxTk1qTzB3UFpKeHp6NUNpTExSVEhvMEl3UURBT0JnTlZIUThCQWY4RQpCQU1DQXFRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBZEJnTlZIUTRFRmdRVStrVk5EZjh1MUJPRnd0VFl3UXBUCkkvSlhkN013Q2dZSUtvWkl6ajBFQXdJRFNBQXdSUUlnUWxpcStYekRqbzZEYURIOGNabXQ2cFlyZCs2d3J2NXgKQXpVVUlZSm8zaklDSVFDU0JDR0E0NzkzQlo1M2dCcE1Qbk5xcHJ2M21lSW1UZmlJaXo2KzNWaGxHdz09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K\r\n    client-key-data: LS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0tCk1IY0NBUUVFSURhZEdTVDZSMHRTSDMrcFhPRFRzSU93RjloRzlvdFdWNjU5QlpEYlF6ZVlvQW9HQ0NxR1NNNDkKQXdFSG9VUURRZ0FFT2R6Z2hVSXN0QmJTOVZxNG9xVjllMU5BTWlRZGkyMVNVRUNxRm1uQVhQQ21pSUUrS2pXRAp1VU9keVFwVGcrd29uN0djVW5NN0FTMTlCbUdTTVYzejlRPT0KLS0tLS1FTkQgRUMgUFJJVkFURSBLRVktLS0tLQo=",
                "podTemplate": "apiVersion: v1\r\nkind: Pod\r\nmetadata:\r\n  name: jobmanager-pod-template\r\nspec:\r\n  initContainers:\r\n    - name: artifacts-fetcher-dinky\r\n      image: docker.1panel.live/library/busybox:latest\r\n      # Use wget or other tools to get user jars from remote storage\r\n      command: [ 'wget', 'http://10.13.8.216:9001/dinky-app-1.20-1.2.0-jar-with-dependencies.jar', '-O', '/flink-usrlib/dinky-app-1.20-1.2.0-jar-with-dependencies.jar' ]\r\n      volumeMounts:\r\n        - mountPath: /flink-usrlib\r\n          name: flink-usrlib\r\n    - name: artifacts-fetcher-mysql\r\n      image: docker.1panel.live/library/busybox:latest\r\n      # Use wget or other tools to get user jars from remote storage\r\n      command: [ 'wget', 'http://10.13.8.216:9001/mysql-connector-java-8.0.30.jar', '-O', '/flink-usrlib/mysql-connector-java-8.0.30.jar' ]\r\n      volumeMounts:\r\n        - mountPath: /flink-usrlib\r\n          name: flink-usrlib\r\n    - name: artifacts-fetcher-hadoop\r\n      image: docker.1panel.live/library/busybox:latest\r\n      # Use wget or other tools to get user jars from remote storage\r\n      command: [ 'wget', 'http://10.13.8.216:9001/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar', '-O', '/flink-usrlib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar' ]\r\n      volumeMounts:\r\n        - mountPath: /flink-usrlib\r\n          name: flink-usrlib\r\n  containers:\r\n    # Do not change the main container name\r\n    - name: flink-main-container\r\n      # env:\r\n      #   - name: FLINK_CONF_DIR\r\n      #     value: /etc/hadoop/conf\r\n      resources:\r\n        requests:\r\n          ephemeral-storage: 2048Mi\r\n        limits:\r\n          ephemeral-storage: 2048Mi\r\n      volumeMounts:\r\n        - mountPath: /opt/flink/usrlib\r\n          name: flink-usrlib\r\n  volumes:\r\n    - name: flink-usrlib\r\n      emptyDir: { }\r\n",
            },
            "clusterConfig": {
                "flinkConfigPath": "/etc/hadoop/conf"
            },
            "flinkConfig": {
                "flinkConfigList": [
                    {
                        "name": "user.artifacts.raw-http-enabled",
                        "value": "true"
                    },
                    {
                        "name": "kubernetes.taskmanager.service-account",
                        "value": "dinky"
                    },
                    {
                        "name": "kubernetes.flink.conf.dir",
                        "value": "/etc/hadoop/conf"
                    }
                ],
                "configuration": {
                    "jobmanager.memory.process.size": "1024mb",
                    "taskmanager.memory.process.size": "1024mb"
                }
            },
            "appConfig": {
                "userJarPath": "http://10.13.8.216:9001/dinky-app-1.20-1.2.0-jar-with-dependencies.jar"
            }
        }
    }
    log.info(f"Adding k8s native application cluster, parameters:{params}")
    return addApplicationCluster(session, params)
