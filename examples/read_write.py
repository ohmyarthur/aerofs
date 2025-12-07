import asyncio
import aerofs


async def main():
    # write
    async with aerofs.open("demo.txt", "w") as f:
        await f.write("Hello World!\n")
        await f.write("Second line here.\n")

    # read
    async with aerofs.open("demo.txt", "r") as f:
        content = await f.read()
        print(content)


if __name__ == "__main__":
    asyncio.run(main())
