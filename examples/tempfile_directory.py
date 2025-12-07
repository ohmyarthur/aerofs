import asyncio
import aerofs


async def main():
    async with aerofs.tempfile.TemporaryDirectory() as tmpdir:
        print(f"temp dir: {tmpdir}")

        filepath = f"{tmpdir}/test.txt"
        async with aerofs.open(filepath, "w") as f:
            await f.write("temp file in temp dir")

        entries = await aerofs.os.listdir(tmpdir)
        print(f"files inside: {entries}")

    print("temp dir auto-deleted after exit")


if __name__ == "__main__":
    asyncio.run(main())
