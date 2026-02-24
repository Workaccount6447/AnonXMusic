# Hardcoded the owner value

from datetime import datetime
from pyrogram import filters
from pyrogram.types import (
LabeledPrice,
InlineKeyboardMarkup,
InlineKeyboardButton,
)
from pyrogram.errors import MessageNotModified

from anony import app

OWNER_ID = 8420494874

------------------ 1. PRE CHECKOUT ------------------

@app.on_pre_checkout_query()
async def pre_checkout_handler(_, query):
await query.answer(ok=True)

------------------ 2. DONATION MENU ------------------

@app.on_callback_query(filters.regex(r"^donate"))
async def donate_menu(, query):
data = query.data.split("")
current_amount = int(data[1]) if len(data) > 1 else 10

text = (  
    "<b>Why should you donate to Royality Bots?\n\n"  
    "• It helps to cover the cost of the servers.\n"  
    "• It motivates us to update and improve the bot.\n"  
    "• Help me buy a cup of tea from Starbucks ☕😅\n\n"  
    "-----------------------------------\n"  
    "👇 <b>Choose an amount to donate:</b></b>"  
)  

buttons = [  
    [  
        InlineKeyboardButton(  
            "-5", callback_data=f"donate_{max(1, current_amount - 5)}"  
        ),  
        InlineKeyboardButton(  
            f"⭐ {current_amount}", callback_data="none"  
        ),  
        InlineKeyboardButton(  
            "+5", callback_data=f"donate_{current_amount + 5}"  
        ),  
    ],  
    [  
        InlineKeyboardButton(  
            "+10 ⭐", callback_data=f"donate_{current_amount + 10}"  
        ),  
        InlineKeyboardButton(  
            "+20 ⭐", callback_data=f"donate_{current_amount + 20}"  
        ),  
    ],  
    [  
        InlineKeyboardButton(  
            "💳 Generate Bill", callback_data=f"bill_{current_amount}"  
        )  
    ],  
    [InlineKeyboardButton("⬅️ Back", callback_data="home")],  
]  

try:  
    await query.message.edit_text(  
        text,  
        reply_markup=InlineKeyboardMarkup(buttons),  
        disable_web_page_preview=True,  
    )  
except MessageNotModified:  
    pass  

await query.answer()

------------------ 3. GENERATE BILL ------------------

@app.on_callback_query(filters.regex(r"^bill_(\d+)$"))
async def send_invoice_bill(, query):
try:
amount = int(query.data.split("")[1])
user = query.from_user

await query.answer(  
        f"✅ Generating bill for {amount} Stars...",  
        show_alert=True,  
    )  

    # Notify Owner (Go to Pay)  
    await app.send_message(  
        OWNER_ID,  
        f"<b>🔔 Donation Initiative</b>\n\n"  
        f"<b>Name :</b> {user.first_name}\n"  
        f"<b>Username :</b> @{user.username if user.username else 'N/A'}\n"  
        f"<b>ID :</b> <code>{user.id}</code>\n"  
        f"<b>Amount :</b> {amount} Stars\n"  
        f"<b>Status :</b> Go to Pay",  
    )  

    # Send Telegram Stars Invoice  
    await app.send_invoice(  
        chat_id=user.id,  
        title="Support Royality Bots ❤️",  
        description=f"Donate {amount} Telegram Stars",  
        payload=f"donate_{amount}",  
        currency="XTR",  # Telegram Stars  
        prices=[LabeledPrice("Donation", amount)],  
        start_parameter="donate-stars",  
    )  

except Exception as e:  
    await query.message.reply_text(f"❌ Error: {e}")

------------------ 4. PAYMENT SUCCESS ------------------

@app.on_message(filters.successful_payment)
async def payment_success(_, message):
try:
payment = message.successful_payment
user = message.from_user

# Notify User  
    await message.reply_text(  
        f"<b>⭐ Payment Successful!</b>\n\n"  
        f"<b>Amount:</b> {payment.total_amount} Stars\n"  
        f"<b>Payment ID:</b> <code>{payment.telegram_payment_charge_id}</code>",  
        reply_markup=InlineKeyboardMarkup(  
            [[InlineKeyboardButton("⬅ Back", callback_data="home")]]  
        ),  
    )  

    # Notify Owner (Paid)  
    await app.send_message(  
        OWNER_ID,  
        f"<b>💰 Donation Received</b>\n\n"  
        f"<b>Name :</b> {user.first_name}\n"  
        f"<b>Username :</b> @{user.username if user.username else 'N/A'}\n"  
        f"<b>ID :</b> <code>{user.id}</code>\n"  
        f"<b>Amount :</b> {payment.total_amount} Stars\n"  
        f"<b>Status :</b> Paid",  
    )  

except Exception as e:  
    print(f"❌ Donation Error: {e}")

