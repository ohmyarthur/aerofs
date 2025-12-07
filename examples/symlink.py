import asyncio
import os
import aerofs


async def main():
    async with aerofs.open("target.txt", "w") as f:
        await f.write("i am the target")

    await aerofs.os.symlink("target.txt", "link.txt")
    print(f"islink(link.txt): {await aerofs.os.path.islink('link.txt')}")
    print(f"readlink: {await aerofs.os.readlink('link.txt')}")

    async with aerofs.open("link.txt", "r") as f:
        print(f"content via link: {await f.read()}")

    os.remove("link.txt")
    os.remove("target.txt")


if __name__ == "__main__":
    asyncio.run(main())
