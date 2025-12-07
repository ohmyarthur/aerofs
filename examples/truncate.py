import asyncio
import aerofs


async def main():
    async with aerofs.open("trunc.txt", "w") as f:
        await f.write("this is a long string that will be truncated")

    async with aerofs.open("trunc.txt", "r+") as f:
        print(f"before: {await f.read()}")

        await f.truncate(10)
        await f.seek(0)
        print(f"after truncate(10): {await f.read()}")


if __name__ == "__main__":
    asyncio.run(main())
