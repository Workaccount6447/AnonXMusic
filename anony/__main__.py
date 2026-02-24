import asyncio
import importlib
import threading
import os
from flask import Flask
from pyrogram import idle
from anony.plugins import donate

from anony import (anon, app, config, db,
                   logger, stop, userbot, yt)
from anony.plugins import all_modules


# ---------- FLASK SERVER ----------
web = Flask(__name__)

@web.route("/")
def home():
    return "AnonXMusic Bot Running"

def run():
    port = int(os.environ.get("PORT", 10000))
    web.run(host="0.0.0.0", port=port)

threading.Thread(target=run).start()
# ----------------------------------


async def main():
    await db.connect()
    await app.boot()
    await userbot.boot()
    await anon.boot()

    for module in all_modules:
        importlib.import_module(f"anony.plugins.{module}")
    logger.info(f"Loaded {len(all_modules)} modules.")

    if config.COOKIES_URL:
        await yt.save_cookies(config.COOKIES_URL)

    sudoers = await db.get_sudoers()
    app.sudoers.update(sudoers)
    app.bl_users.update(await db.get_blacklisted())
    logger.info(f"Loaded {len(app.sudoers)} sudo users.")

    await idle()
    await stop()


if __name__ == "__main__":
    try:
        asyncio.get_event_loop().run_until_complete(main())
    except KeyboardInterrupt:
        pass