import asyncio
import aerofs


async def main():
    # create nested dirs
    await aerofs.os.makedirs("test_dir/sub/deep", exist_ok=True)
    print("created nested directories")

    # list directory
    entries = await aerofs.os.listdir("test_dir")
    print(f"listdir: {entries}")

    # cleanup
    await aerofs.os.rmdir("test_dir/sub/deep")
    await aerofs.os.rmdir("test_dir/sub")
    await aerofs.os.rmdir("test_dir")
    print("cleaned up")


if __name__ == "__main__":
    asyncio.run(main())
