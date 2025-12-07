import asyncio
import aerofs


async def main():
    cwd = await aerofs.os.getcwd()
    print(f"cwd: {cwd}")

    abspath = await aerofs.os.path.abspath(".")
    print(f"abspath(.): {abspath}")


if __name__ == "__main__":
    asyncio.run(main())
