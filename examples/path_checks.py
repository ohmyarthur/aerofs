import asyncio
import aerofs


async def main():
    async with aerofs.open("check.txt", "w") as f:
        await f.write("test content")

    await aerofs.os.makedirs("check_dir", exist_ok=True)

    print(f"exists(check.txt): {await aerofs.os.path.exists('check.txt')}")
    print(f"isfile(check.txt): {await aerofs.os.path.isfile('check.txt')}")
    print(f"isdir(check_dir): {await aerofs.os.path.isdir('check_dir')}")
    print(f"getsize(check.txt): {await aerofs.os.path.getsize('check.txt')}")
    print(f"exists(nope.txt): {await aerofs.os.path.exists('nope.txt')}")

    await aerofs.os.remove("check.txt")
    await aerofs.os.rmdir("check_dir")


if __name__ == "__main__":
    asyncio.run(main())
