import asyncio
import aerofs


async def main():
    async with aerofs.open("seek.txt", "w") as f:
        await f.write("0123456789ABCDEF")

    async with aerofs.open("seek.txt", "r") as f:
        pos = await f.tell()
        print(f"start position: {pos}")

        data = await f.read(5)
        print(f"read 5 chars: {data}")

        pos = await f.tell()
        print(f"after read: {pos}")

        await f.seek(10)
        data = await f.read(3)
        print(f"seek(10), read 3: {data}")

        await f.seek(0)
        print(f"back to start: {await f.tell()}")


if __name__ == "__main__":
    asyncio.run(main())
