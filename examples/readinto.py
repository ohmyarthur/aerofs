import asyncio
import aerofs


async def main():
    async with aerofs.open("readinto.bin", "wb") as f:
        await f.write(b"hello world!")

    async with aerofs.open("readinto.bin", "rb") as f:
        buf = bytearray(5)
        n = await f.readinto(buf)
        print(f"read {n} bytes into buffer: {buf}")


if __name__ == "__main__":
    asyncio.run(main())
