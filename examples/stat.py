import asyncio
import aerofs


async def main():
    async with aerofs.open("stat_test.txt", "w") as f:
        await f.write("some content")

    stat = await aerofs.os.stat("stat_test.txt")
    print(f"size: {stat.st_size}")
    print(f"mode: {oct(stat.st_mode)}")
    print(f"uid: {stat.st_uid}")
    print(f"mtime: {stat.st_mtime}")


if __name__ == "__main__":
    asyncio.run(main())
