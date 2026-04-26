import os
import re

import discord
import httpx

TOKEN = os.environ["DISCORD_TOKEN"]
FASTAPI_URL = os.environ["FASTAPI_URL"]
INBOX_CHANNEL_ID = int(os.environ["INBOX_CHANNEL_ID"])

URL_PATTERN = re.compile(r"https?://[^\s>]+")

intents = discord.Intents.default()
intents.message_content = True

bot = discord.Bot(intents=intents)


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (id: {bot.user.id})")


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if message.channel.id != INBOX_CHANNEL_ID:
        return

    urls = URL_PATTERN.findall(message.content)
    if not urls:
        return

    url = urls[0]
    await message.add_reaction("⏳")

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"{FASTAPI_URL}/jobs",
                json={
                    "url": url,
                    "channel_id": str(message.channel.id),
                    "message_id": str(message.id),
                },
                timeout=10,
            )
            if resp.status_code != 202:
                await message.remove_reaction("⏳", bot.user)
                await message.add_reaction("❌")
        except Exception as e:
            print(f"FastAPI unreachable: {e}")
            await message.clear_reactions()
            await message.add_reaction("❌")


bot.run(TOKEN)
