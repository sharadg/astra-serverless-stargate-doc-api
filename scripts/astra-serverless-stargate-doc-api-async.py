import json
from requests.auth import AuthBase
import aiohttp
import asyncio
import requests
import time
import os
# from fake_json import generate_record

# Astra Cluster ID
astraDbId = ""
# Astra DB region
astraRegion = ""
# Astra DB keyspace
astraKeyspace = ""
# Astra collection (think of it like a table) to create
astraCollection = ""
# App token
astraAppToken = ""

###Create collection###


def create_collection():
    http_request = "https://" + astraDbId + "-" + astraRegion + \
        ".apps.astra.datastax.com/api/rest/v2/namespaces/" + astraKeyspace + "/collections"
    headers = {"Content-Type": "application/json",
               "x-cassandra-token": astraAppToken, "accept": "application/json"}
    
    print(f"Creating collection -> {astraCollection} <- since it doesn't exist")

    response_post_collection = requests.post(
        http_request, headers=headers, json={"name": astraCollection})
    if (response_post_collection.status_code != 201):
        # try again or error out
        print(response_post_collection)
        exit(1)

    print(response_post_collection)  # 201 --> Good

###Check for existing collection###


def exists_collection():
    http_request = "https://" + astraDbId + "-" + astraRegion + ".apps.astra.datastax.com/api/rest/v2/namespaces/" + \
        astraKeyspace + "/collections/" + astraCollection
    headers = {"x-cassandra-token": astraAppToken}
    print("Checking to see whether the collection exists...")
    response_get_collection = requests.get(http_request, headers=headers)
    if (response_get_collection.status_code != 200):
        # try again or error out
        return False

    return True


###Load documents in to collection (Person)###

async def post_payload(session, http_request, headers, json_data):
    async with session.post(http_request, headers=headers, json=json_data) as response:
        pass
        # print(response.status)  # 201 --> Good


async def load_json(session, data):
    http_request = "https://" + astraDbId + "-" + astraRegion + ".apps.astra.datastax.com/api/rest/v2/namespaces/" + \
        astraKeyspace + "/collections/" + astraCollection
    headers = {"Content-Type": "application/json", "X-Cassandra-Token": astraAppToken,
               "accept": "application/json"}

    tasks = []
    for data_item in data:
        task = asyncio.create_task(post_payload(
            session, http_request, headers, data_item))
        tasks.append(task)
    await asyncio.gather(*tasks, return_exceptions=True)


async def main():
    # Json data to load
    with open(os.path.join(os.path.dirname(__file__), '../data/MOCK_DATA_PERSON.json')) as f:
        people_data = json.load(f)

    with open(os.path.join(os.path.dirname(__file__), '../data/MOCK_DATA_CAR.json')) as g:
        car_data = json.load(g)

    # async with aiohttp.ClientSession() as session:
    #     async for j in generate_record():
    #         await load_json(session, json.loads(j))

    async with aiohttp.ClientSession() as session:
        await load_json(session, people_data * 50)
        print(f"People data loaded...({len(people_data) * 50}) records")
        await (load_json(session, car_data * 50))
        print(f"Car data loaded...({len(car_data) * 50}) records")


if __name__ == "__main__":

    start_time = time.time()
    if (not exists_collection()):
        create_collection()
    duration = time.time() - start_time
    print(f"Collection check completed in {duration} seconds")

    start_time = time.time()
    asyncio.run(main())
    duration = time.time() - start_time
    print(f"Data loaded in {duration} seconds")
