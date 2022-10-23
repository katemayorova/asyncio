import asyncio
from aiohttp import ClientSession
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker
import more_itertools
from sqlalchemy import Column, Integer, String


PG_DSN = 'postgresql+asyncpg://postgres:1qaz!QAZ@127.0.0.1:5432/people'
engine = create_async_engine(PG_DSN)
Base = declarative_base(bind=engine)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)

    @staticmethod
    def create(json_dict):
        new_create = People(birth_year=json_dict['birth_year'],
                            eye_color=json_dict['eye_color'], films=", ".join(json_dict['films']),
                            gender=json_dict['gender'], hair_color=json_dict['hair_color'],
                            height=json_dict['height'], homeworld=json_dict['homeworld'],
                            mass=json_dict['mass'], name=json_dict['name'],
                            skin_color=json_dict['skin_color'], species=", ".join(json_dict['species']),
                            starships=", ".join(json_dict['starships']), vehicles=", ".join(json_dict['vehicles']))
        return new_create


CHUNK_SIZE = 10


async def foo(tasks_chunk):
    for task in tasks_chunk:
        result = await task
        yield result


async def get_people(people_id, session):
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        return await response.json()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    async with ClientSession() as http_session:
        tasks = (asyncio.create_task(get_people(i, http_session)) for i in range(1, 82))
        for tasks_chunk in more_itertools.chunked(tasks, CHUNK_SIZE):
            async with Session() as db_session:
                async for result in foo(tasks_chunk):
                    people = People.create(result)
                    db_session.add(people)
                await db_session.commit()


asyncio.run(main())
