import asyncio

from vitrine.core.database import get_session


async def main():
    async for session in get_session():
        result = await session.execute()
        rows = result.fetchall()
        print(rows)


if __name__ == '__main__':
    asyncio.run(main())
