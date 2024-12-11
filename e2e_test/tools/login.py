import sys
from json import JSONDecodeError

import requests
from requests import Response

dinky_addr = sys.argv[1]


def url(path: str):
    return rf"http://{dinky_addr}/{path}"


def assertRespOk(resp: Response, api_name: str):
    if resp.status_code != 200:
        raise AssertionError("api name:{api_name} request failed")
    else:
        try:
            resp_json = resp.json()
            if not resp_json["success"]:
                raise AssertionError(f"api name:{api_name} request failed.Error: {resp_json['msg']}")
        except JSONDecodeError as e:
            raise AssertionError(f"api name:{api_name} request failed.Error: {resp.content.decode()}")


def login(session: requests.Session):
    login_resp: Response = session.post(url("api/login"),
                                        json={"username": "admin", "password": "dinky123!@#", "ldapLogin": False,
                                              "autoLogin": True})
    assertRespOk(login_resp, "Login")

    choose_tenant_resp = session.post(url("api/chooseTenant?tenantId=1"))
    assertRespOk(choose_tenant_resp, "Choose Tenant")
    session.cookies.set("tenantId", '1')



