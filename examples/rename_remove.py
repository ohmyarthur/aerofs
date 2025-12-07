import asyncio
import aerofs


async def main():
    async with aerofs.open("original.txt", "w") as f:
        await f.write("testing rename")

    await aerofs.os.rename("original.txt", "renamed.txt")
    print("renamed to renamed.txt")

    exists = await aerofs.os.path.exists("renamed.txt")
    print(f"renamed.txt exists: {exists}")

    await aerofs.os.remove("renamed.txt")
    print("removed")


if __name__ == "__main__":
    asyncio.run(main())
