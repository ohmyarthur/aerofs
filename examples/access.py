import asyncio
import os
import aerofs


async def main():
    async with aerofs.open("access_test.txt", "w") as f:
        await f.write("test")

    print(f"F_OK (exists): {await aerofs.os.access('access_test.txt', os.F_OK)}")
    print(f"R_OK (readable): {await aerofs.os.access('access_test.txt', os.R_OK)}")
    print(f"W_OK (writable): {await aerofs.os.access('access_test.txt', os.W_OK)}")
    print(f"X_OK (executable): {await aerofs.os.access('access_test.txt', os.X_OK)}")

    await aerofs.os.remove("access_test.txt")


if __name__ == "__main__":
    asyncio.run(main())
