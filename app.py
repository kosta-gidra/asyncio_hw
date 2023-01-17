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
            cor_vehicles = []
            for vehicles_link in json_data["vehicles"]:
                json_vehicles = get_json_from_link(vehicles_link, session, name="name")
                cor_vehicles.append(json_vehicles)
            vehicles = await asyncio.gather(*cor_vehicles)

            cor_starships = []
            for starships_link in json_data["starships"]:
                json_starships = get_json_from_link(starships_link, session, name="name")
                cor_starships.append(json_starships)
            starships = await asyncio.gather(*cor_starships)

            cor_species = []
            for species_link in json_data["species"]:
                json_species = get_json_from_link(species_link, session, name="name")
                cor_species.append(json_species)
            species = await asyncio.gather(*cor_species)

            cor_films = []
            for films_link in json_data["films"]:
                json_films = get_json_from_link(films_link, session, name="title")
                cor_films.append(json_films)
            films = await asyncio.gather(*cor_films)

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


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 83), CHUNK_SIZE):
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
