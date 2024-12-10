import requests

from login import assertRespOk, url


class CatalogueTree:
    def __init__(self, id: int, name: str, taskId: int, children):
        self.id = id
        self.name = name
        self.taskId = taskId
        self.children: list[CatalogueTree] = children


def getTask(data_list: list[dict], name: str) -> CatalogueTree:
    for data in data_list:
        if data['name'] == name:
            return CatalogueTree(data['id'], data['name'], data['taskId'], data['children'])
        if len(data["children"]) > 0:
            result = getTask(data["children"], name)
            if result is not None:
                return result


def addCatalogue(session: requests.Session, name: str, isLeaf: bool = False, parentId: int = 0):
    add_parent_dir_resp = session.put(url("api/catalogue/saveOrUpdateCatalogue"), json={
        "name": name,
        "isLeaf": isLeaf,
        "parentId": parentId
    })
    assertRespOk(add_parent_dir_resp, "创建主测试路径")
    get_all_tasks_resp = session.post(url("api/catalogue/getCatalogueTreeData"), json={
        "sortValue": "",
        "sortType": ""
    })
    assertRespOk(get_all_tasks_resp, "获取作业详情")
    data_list: list[dict] = get_all_tasks_resp.json()['data']
    return getTask(data_list, name)


def addTask(session: requests.Session, name: str, parentId: int = 0, type: str = "FlinkSql", statement: str = ""):
    add_parent_dir_resp = session.put(url("api/catalogue/saveOrUpdateCatalogueAndTask"), json={
        "name": name,
        "type": type,
        "firstLevelOwner": 1,
        "task": {
            "savePointStrategy": 0,
            "parallelism": 1,
            "envId": -1,
            "step": 1,
            "alertGroupId": -1,
            "type": "local",
            "dialect": type,
            "statement": statement,
            "firstLevelOwner": 1
        },
        "isLeaf": False,
        "parentId": parentId
    })
    assertRespOk(add_parent_dir_resp, "创建作业")
    get_all_tasks_resp = session.post(url("api/catalogue/getCatalogueTreeData"), json={
        "sortValue": "",
        "sortType": ""
    })
    assertRespOk(get_all_tasks_resp, "获取作业详情")
    data_list: list[dict] = get_all_tasks_resp.json()['data']
    return getTask(data_list, name)


def runTask(session: requests.Session, taskId: int):
    run_task_resp = session.get(url(f"api/task/submitTask?id={taskId}"))
    assertRespOk(run_task_resp, "运行Task")
