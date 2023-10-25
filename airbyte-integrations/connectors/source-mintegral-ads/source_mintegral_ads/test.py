import time
import hashlib
import requests
import pendulum

api_key = "f7cb650a82d1b1ccda692118a6881d88"
access_key = "71c6167f39f4f75e20f5b33675ee5753"

now = int(time.time())
token = hashlib.md5((api_key + hashlib.md5(str(now).encode()).hexdigest()).encode()).hexdigest()

response_1 = requests.get(
    url='https://ss-api.mintegral.com/api/open/v1/campaign',
    headers={
        "access-key": access_key,
        "token": token,
        "timestamp": str(now)
    },
    params={
        'limit': 50,
        'page': 1
    }

)
json = response_1.json()
print(json)
list_cam = []
for i in json['data']['list']:
    list_cam.append(i['campaign_id'])
print(list_cam)
for x in list_cam:
    response_2 = requests.get(
        url='https://ss-api.mintegral.com/api/v1/reports/data',
        headers={
            "access-key": access_key,
            "token": token,
            "timestamp": str(now)
        },
        params={
            'utc': '7',
            'start_date': '2023-10-01',
            'end_date': '2023-10-07',
            'dimension': 'location',
            'campaign_id': x,
            'not_empty_field':'click,install,impression,spend'
        }

    )
    json = response_2.json()
    print(json)

