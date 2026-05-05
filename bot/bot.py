import logging
logger = logging.getLogger(__name__)

import discord
from discord.ext import commands
from bot.cogs import download_video as cog_download_video
import redis.asyncio as redis

from utils.config import REDIS_CONN_ARGS

intents = discord.Intents.default()
intents.guilds = True
intents.message_content = True

cogs = [cog_download_video]

class DLBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="&", intents=intents)

    async def setup_hook(self):
        await self.tree.sync()

bot = DLBot()

async def init_bot():
    logger.debug("Setting up the redis client")
    redis_aclient = redis.Redis(**REDIS_CONN_ARGS)
    bot.redis = redis_aclient
    logger.debug("Initializing cogs")
    await init_cogs()
    logger.info("Done initializing the bot")

async def init_cogs():
    for cog in cogs:
        logger.debug(f"Loading cog {cog.__name__}")
        await cog.setup(bot)
