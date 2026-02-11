import discord
from discord.ext import commands
from bot.cogs import download_video as cog_download_video
import redis.asyncio as redis

intents = discord.Intents.default()
intents.guilds = True
intents.message_content = True

class DLBot(commands.Bot):
    def __init__(self):
        super().__init(command_prefix="&", intents=intents)

    async def setup_hook(self):
        await self.tree.sync()

#bot = commands.Bot(command_prefix="&", intents=intents)
bot = DLBot()

async def init_bot():
    redis_aclient = redis.Redis()
    bot.redis = redis_aclient
    await init_cogs()

async def init_cogs():
    await cog_download_video.setup(bot)
