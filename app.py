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
        json_data = await response.json()
    print(f'end {people_id}')
    return json_data


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 21), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)
            for item in results:
                yield item


async def insert_people(people_chunk):
    async with Session() as session:
        session.add_all([People(
            birth_year=item.get("birth_year"),
            eye_color=item.get("eye_color"),
            films=",".join(item.get("films", "None")),
            gender=item.get("gender"),
            hair_color=item.get("hair_color"),
            height=item.get("height"),
            homeworld=item.get("homeworld"),
            mass=item.get("mass"),
            name=item.get("name"),
            skin_color=item.get("skin_color"),
            species=",".join(item.get("species", "None")),
            starships=",".join(item.get("starships", "None")),
            vehicles=",".join(item.get("vehicles", "None"))
        ) for item in people_chunk])
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