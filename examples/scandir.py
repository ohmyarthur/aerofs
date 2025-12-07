import asyncio
import aerofs


async def main():
    await aerofs.os.makedirs("scan_test", exist_ok=True)

    async with aerofs.open("scan_test/file1.txt", "w") as f:
        await f.write("hello")
    async with aerofs.open("scan_test/file2.txt", "w") as f:
        await f.write("world")

    entries = await aerofs.os.scandir("scan_test")
    for entry in entries:
        print(f"{entry.name}: is_file={entry.is_file()}, is_dir={entry.is_dir()}")

    # cleanup
    await aerofs.os.remove("scan_test/file1.txt")
    await aerofs.os.remove("scan_test/file2.txt")
    await aerofs.os.rmdir("scan_test")


if __name__ == "__main__":
    asyncio.run(main())
