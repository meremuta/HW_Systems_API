import json
import ydb
import os
import pandas as pd

driver_config = ydb.DriverConfig(
    endpoint=os.getenv("YDB_ENDPOINT"),
    database=os.getenv("YDB_DATABASE"),
    credentials=ydb.iam.MetadataUrlCredentials()
)

driver = ydb.Driver(driver_config)
# Wait for the driver to become active for requests.
driver.wait(fail_fast=True, timeout=5)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(driver)
def upsert_userrating(uid, anime_id, ip, rating):
    text = f"UPSERT INTO userrating (`id`, `anime_id`, `ip`, `rating`) VALUES ( '{uid}', '{anime_id}', '{ip}', '{rating}') ;"
    return pool.retry_operation_sync(lambda s: s.transaction().execute(
        text,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))


def select_userrating(ip):
    text = f"SELECT `anime_id`, `rating` FROM  userrating WHERE `ip`== '{ip}';"
    user_data_ydb = pool.retry_operation_sync(
        lambda s: s.transaction().execute(
            text,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    )
    user_data = pd.DataFrame.from_records(user_data_ydb[0].rows)
    return user_data


def delete_userrating(anime_id, ip):
    text = f"DELETE FROM userrating where `anime_id` == '{anime_id}' AND `ip` == '{ip}';"
    return pool.retry_operation_sync(lambda s: s.transaction().execute(
        text,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))


def handler(event, context):
    response = json.loads(json.dumps(event))
    method = response['httpMethod']
    if method == 'POST':
        try:
            anime_id = response['multiValueParams']['ID'][0]
            rating = response['multiValueParams']['rate'][0]
            ip = response['headers']['X-Envoy-External-Address']
            uid = ip + '_' + anime_id
            upsert_userrating(uid, anime_id, ip, rating)
            result = "Succesfully updated"
        except:
            result = 'Something is wrong with parameters'
    elif method == 'GET':
        try:
            uid = response['headers']['X-Envoy-External-Address']
            userdata = select_userrating(uid)
            result = userdata.to_json(orient='records')

        except:
            result = 'Something is wrong with address'
    elif method == 'DELETE':
        try:
            anime_id = response['multiValueParams']['ID'][0]
            ip = response['headers']['X-Envoy-External-Address']
            delete_userrating(anime_id, ip)
            result = "Succesfully deleted"
        except:
            result = 'Something is wrong with deleting record'
    else:
        result = 'Something is wrong with method'

    print(event)
    return {"statusCode": 200,
            "body": result
            }