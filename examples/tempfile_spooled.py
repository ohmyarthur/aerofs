import asyncio
import aerofs


async def main():
    f = aerofs.tempfile.SpooledTemporaryFile(max_size=1024, mode="w+b")

    await f.write(b"small data")
    await f.seek(0)
    content = await f.read()
    print(f"content: {content}")

    await f.close()


if __name__ == "__main__":
    asyncio.run(main())
