import os
import asyncio
from bot.bot import bot, init_bot

async def main():
    token = os.environ.get("DISCORD_TOKEN", "")
    if not token:
        raise EnvironmentError("No environment variable set for DISCORD_TOKEN")
    await init_bot()

    async with bot:
        await bot.start(token)

if __name__ == "__main__":
    asyncio.run(main())
