import asyncio
import json

import aiohttp
import os.path

from dataclasses import dataclass
from aiohttp import ClientSession

FETCH_PROGRAMMING_LANGUAGES = False

@dataclass
class ListContributorsRequest:
    repo: str
    url: str

@dataclass
class ListLanguagesRequest:
    repo:str
    url:str

@dataclass
class UserContributionInfo:
    login: str
    repo: str
    contributions: int

def __url_to_valid_file_name(url:str):
    result = url.replace("/", "_").replace(":","_").replace("?","_")
    return result

async def __iterate_github_responses_async(http: ClientSession, url:str):

    async def __iterate(http: ClientSession, links: dict[str, str]) -> tuple[dict[str, str], dict]:
        async with http.get(links['next']) as resp:

            next_links = {}

            if "Link" in resp.headers:
                for link in resp.headers["Link"].split(","):
                    url_raw, rel_raw = link.split(";")
                    next_rel = rel_raw.split("=")[1].replace('"', "")
                    next_links[next_rel] = url_raw.strip()[1:-1]

            items = await resp.json()
            return next_links, items

    links = {
        "next": f"{url}?per_page=100"
    }

    while 'next' in links:
        links, items = await __iterate(http, links)
        for item in items:
            yield item

async def __try_iterate_from_cache_async(http: ClientSession, url:str):

    filename = f".cache/{__url_to_valid_file_name(url)}"

    if os.path.exists(filename):
        print(f"Cache for {url} exists")

        file = open(filename, "r")

        while True:
            line = file.readline()
            if not line:
                break

            item = json.loads(line)
            yield item
    else:
        print(f"Loading {url} from the external source")
        file = open(filename, "w")
        async for item in __iterate_github_responses_async(http, url):
            file.write(f"{json.dumps(item)}\n")
            yield item

async def api_list_repositories_async(http: ClientSession, base_url:str):

    empty_repos = []

    async for item in __try_iterate_from_cache_async(http, f"{base_url}/repos?type=private"):

        if item["size"] == 0:
            empty_repos.append(item["name"])

        yield item

    print(len(empty_repos))
    print(empty_repos)


async def api_fetch_contributors_async(http: ClientSession, request: ListContributorsRequest):
    return __try_iterate_from_cache_async(http, f"{request.url}")


async def process_repo_info_async(item, fetch_contributors_queue, fetch_languages_queue):
    is_fork = item["fork"]
    name = item["name"]

    if is_fork:
        print(f"Repo '{name}' is a fork, ignoring")
        return

    is_empty = item["size"] == 0
    if is_empty:
        print(f"Repo '{name}' is empty, ignoring")
        return

    contributors_url = item["contributors_url"]
    languages_url = item["languages_url"]

    await fetch_contributors_queue.put(ListContributorsRequest(
        repo = name,
        url = contributors_url
    ))

    if FETCH_PROGRAMMING_LANGUAGES:
        await fetch_languages_queue.put(ListLanguagesRequest(
            repo=name,
            url = languages_url
        ))

async def consume_list_contributors_request_queue_async(http: ClientSession, fetch_contributors_queue: asyncio.Queue):
    while True:
        request:ListContributorsRequest = await fetch_contributors_queue.get()
        print(f"Processing request for repo {request.repo}, {fetch_contributors_queue.qsize()} more left")

        try:
            async for item in await api_fetch_contributors_async(http, request):
                new = item

        except Exception as e:
            print(f"ERROR for {request.repo}: {e}")
        finally:
            fetch_contributors_queue.task_done()

async def run(org, token):

    fetch_contributors_queue = asyncio.Queue()
    fetch_languages_queue = asyncio.Queue()

    async with aiohttp.ClientSession(headers={"Authorization": f"token {token}"}) as http:

        async for item in api_list_repositories_async(http, f"https://api.github.com/orgs/{org}"):
            await process_repo_info_async(item, fetch_contributors_queue, fetch_languages_queue)

        fetch_contributors_consumers = [asyncio.create_task(consume_list_contributors_request_queue_async(http, fetch_contributors_queue))
                                        for _ in range(0, 5)]
        await fetch_contributors_queue.join()


if __name__ == "__main__":
    import platform

    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(run("<organization>", "<token>"))