import discord
from discord.ext import commands
from bot.cogs import download_video as cog_download_video

intents = discord.Intents.default()
intents.guilds = True
intents.message_content = True

bot = commands.Bot(command_prefix="&", intents=intents)

async def init_bot():
    await init_cogs()

async def init_cogs():
    await cog_download_video.setup(bot)
