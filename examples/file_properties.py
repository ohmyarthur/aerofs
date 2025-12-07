import asyncio
import aerofs


async def main():
    async with aerofs.open("props.txt", "w") as f:
        print(f"name: {f.name}")
        print(f"mode: {f.mode}")
        print(f"closed: {f.closed}")
        print(f"readable: {f.readable()}")
        print(f"writable: {f.writable()}")
        print(f"seekable: {f.seekable()}")

    await aerofs.os.remove("props.txt")


if __name__ == "__main__":
    asyncio.run(main())
