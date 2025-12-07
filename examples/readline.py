import asyncio
import aerofs


async def main():
    # create sample file
    async with aerofs.open("lines.txt", "w") as f:
        await f.write("line 1\nline 2\nline 3\n")

    # readline
    async with aerofs.open("lines.txt", "r") as f:
        line = await f.readline()
        print(f"First: {line.strip()}")
        
        line = await f.readline()
        print(f"Second: {line.strip()}")

    # readlines
    async with aerofs.open("lines.txt", "r") as f:
        lines = await f.readlines()
        print(f"Total lines: {len(lines)}")


if __name__ == "__main__":
    asyncio.run(main())
