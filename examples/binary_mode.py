import asyncio
import aerofs


async def main():
    data = bytes([0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD])

    async with aerofs.open("binary.bin", "wb") as f:
        await f.write(data)
        print(f"wrote {len(data)} bytes")

    async with aerofs.open("binary.bin", "rb") as f:
        content = await f.read()
        print(f"read: {content.hex()}")


if __name__ == "__main__":
    asyncio.run(main())
