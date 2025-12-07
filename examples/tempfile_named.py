import asyncio
import aerofs


async def main():
    async with aerofs.tempfile.NamedTemporaryFile(mode="w+b") as f:
        print(f"temp file: {f.name}")
        await f.write(b"temp data here")
        await f.seek(0)
        content = await f.read()
        print(f"content: {content}")

    print("temp file auto-deleted after exit")


if __name__ == "__main__":
    asyncio.run(main())
