import asyncio
import datetime
from aiohttp import ClientSession
from more_itertools import chunked
from models import engine, Session, People, Base

CHUNK_SIZE = 10


async def chunked_async(async_iter, size):
    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            yield buffer
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_person(people_id: int, session: ClientSession):
    print(f'begin {people_id}')
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        status = response.status
        json_data = await response.json()
        films = []
        species = []
        starships = []
        vehicles = []
        if status == 200:
            films = await get_cor(items_list=json_data["films"], session=session, name="title")
            species = await get_cor(items_list=json_data["species"], session=session, name="name")
            starships = await get_cor(items_list=json_data["starships"], session=session, name="name")
            vehicles = await get_cor(items_list=json_data["vehicles"], session=session, name="name")

    print(f'end {people_id}')
    return {"status": status,
            "json_data": json_data,
            "films": films,
            "species": species,
            "starships": starships,
            "vehicles": vehicles
            }


async def get_json_from_link(link: str, session: ClientSession, name: str):
    print(f'begin {link}')
    async with session.get(link) as response:
        json_data = await response.json()
    print(f'end {link}')
    return json_data[name]


async def get_cor(items_list: list, session: ClientSession, name: str):
    cor = []
    for link in items_list:
        items_json = get_json_from_link(link, session, name=name)
        cor.append(items_json)
    result = await asyncio.gather(*cor)
    return result


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 85), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)
            for item in results:
                yield item


async def insert_people(people_chunk):
    async with Session() as session:
        people_list = []
        for item in people_chunk:
            if item["status"] == 200:
                people_list.append(People(birth_year=item["json_data"]["birth_year"],
                                          eye_color=item["json_data"]["eye_color"],
                                          gender=item["json_data"]["gender"],
                                          hair_color=item["json_data"]["hair_color"],
                                          height=item["json_data"]["height"],
                                          homeworld=item["json_data"]["homeworld"],
                                          mass=item["json_data"]["mass"],
                                          name=item["json_data"]["name"],
                                          skin_color=item["json_data"]["skin_color"],
                                          films=",".join(item["films"]),
                                          species=",".join(item["species"]),
                                          starships=",".join(item["starships"]),
                                          vehicles=",".join(item["vehicles"])
                                          ))
        session.add_all(people_list)
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for chunk in chunked_async(get_people(), CHUNK_SIZE):
        asyncio.create_task(insert_people(chunk))

    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
