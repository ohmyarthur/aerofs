import asyncio
import aerofs


async def write_file(name, content):
    async with aerofs.open(name, "w") as f:
        await f.write(content)


async def read_file(name):
    async with aerofs.open(name, "r") as f:
        return await f.read()


async def main():
    files = [f"concurrent_{i}.txt" for i in range(5)]
    
    # write all concurrently
    await asyncio.gather(*[
        write_file(f, f"content of {f}")
        for f in files
    ])
    print("wrote 5 files concurrently")

    # read all concurrently
    contents = await asyncio.gather(*[read_file(f) for f in files])
    for f, c in zip(files, contents):
        print(f"{f}: {c}")

    # cleanup
    for f in files:
        await aerofs.os.remove(f)


if __name__ == "__main__":
    asyncio.run(main())
