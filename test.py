import requests

events_store_url = "http://127.0.0.1:8020"
recommendations_url = "http://127.0.0.1:8000"

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
# params = [{"user_id": 1127794, "item_id": 18734992},
#           {"user_id": 1127794, "item_id": 18734992},
#           {"user_id": 1127794, "item_id": 7785},
#           {"user_id": 1127794, "item_id": 4731479}]

# for param in params:
#     resp = requests.post(events_store_url + "/put", headers=headers, params=param)
#     if resp.status_code == 200:
#         result = resp.json()
#     else:
#         result = None
#         print(f"status code: {resp.status_code}")

# resp = requests.post(events_store_url + "/get", 
#                      headers=headers, 
#                      params={"user_id": 1127794, "k": 3})
# print(resp.json())

user_id = 1291250
event_item_ids =  [7144, 16299, 5907, 18135]

for event_item_id in event_item_ids:
    resp = requests.post(events_store_url + "/put", 
                         headers=headers, 
                         params={"user_id": user_id, "item_id": event_item_id})
                         
params = {"user_id": 1291250, 'k': 10}
resp_offline = requests.post(recommendations_url + "/recommendations_offline", headers=headers, params=params)
resp_online = requests.post(recommendations_url + "/recommendations_online", headers=headers, params=params)
resp_blended = requests.post(recommendations_url + "/recommendations", headers=headers, params=params)

try:
    recs_offline = resp_offline.json()["recs"]
except:
    print(resp_offline.json())

try:
    recs_online = resp_online.json()["recs"]
except:
    print(resp_online.json())

try:
    recs_blended = resp_blended.json()["recs"]
except:
    print(resp_blended.json())

print(recs_offline)
print(recs_online)
print(recs_blended) 