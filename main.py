import logging
import os
import asyncio
import sys
from bot.bot import bot, init_bot

logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s] [%(name)s.%(funcName)s] %(levelname)s: %(message)s",
        stream=sys.stdout
)
logging.getLogger("discord").setLevel(logging.WARNING)

async def main():
    token = os.environ.get("DISCORD_TOKEN", "")
    if not token:
        raise EnvironmentError("No environment variable set for DISCORD_TOKEN")
    await init_bot()

    async with bot:
        await bot.start(token)

if __name__ == "__main__":
    asyncio.run(main())
