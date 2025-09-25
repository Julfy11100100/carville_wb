import asyncio
from pprint import pprint

from app.utils.logging import get_logger

logger = get_logger()

import aiohttp

BASE_URL = "https://content-api-sandbox.wildberries.ru"
API_TOKEN = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwOTA0djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc3NDI5NTc0OCwiaWQiOiIwMTk5NzA2Yi1mODlhLTdjMjctODdhYi05MTljMDM2ZTM5YjMiLCJpaWQiOjI1NDE3NTUxMywib2lkIjoxMDIzODQsInMiOjAsInNpZCI6IjdhZWI3MTlhLWNiOGQtNWY3OS04YThlLWI1ZjBmZTQ0N2YxNCIsInQiOnRydWUsInVpZCI6MjU0MTc1NTEzfQ.mjAMLMvqJBzpVlnuU11Gpsmgytg0PGIwcbD0hggepK3L3SGuH-J5jieenn11G-QDAAEnZuijk-n3E-lZUAdUEw"

headers = {"Authorization": API_TOKEN}


async def fetch_parents(session):
    url = f"{BASE_URL}/content/v2/object/parent/all"
    resp = await session.get(url, headers=headers)
    json_result = await resp.json()
    logger.info(f"fetch_parents.json_result = {json_result}")
    result = json_result["data"]
    return result


async def fetch_children(session, parent_id):
    url = f"{BASE_URL}/content/v2/object/all"
    params = {"parentID": parent_id, "limit": 1000, "offset": 0}
    resp = await session.get(url, headers=headers, params=params)
    json_result = (await resp.json())
    logger.info(f"fetch_children.json_result = {json_result}")
    result = json_result["data"]
    return result


async def build_category_tree():
    async with aiohttp.ClientSession() as session:
        parents = await fetch_parents(session)
        await asyncio.sleep(2)  # для соблюдения rate limit
        tree = {}
        for parent in parents:

            parent_id = parent["id"]
            tree[parent["name"]] = {
                "id": parent_id,
                "subcategorys": []
            }

            children = await fetch_children(session, parent_id)
            if children:
                subcategory = [{
                    "id": c["subjectID"],
                    "name": c["subjectName"]
                } for c in children]

                tree[parent["name"]]["subcategorys"] = subcategory

            await asyncio.sleep(1)  # для соблюдения rate limit
        return tree


if __name__ == "__main__":
    tree = asyncio.run(build_category_tree())
    pprint(tree)
