import asyncio
import aerofs


async def main():
    async with aerofs.open("iter.txt", "w") as f:
        await f.write("alpha\nbeta\ngamma\ndelta\n")

    async with aerofs.open("iter.txt", "r") as f:
        async for line in f:
            print(line.strip())


if __name__ == "__main__":
    asyncio.run(main())
