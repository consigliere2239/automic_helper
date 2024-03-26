import constants as keys
import telegram_bot_main as backend
from telegram.ext import *
from telegram import Bot, Update
import string

global ozet
ozet=0

print("Bot Started")


def start_command(update, context):
    update.message.reply_text('Consigliere automic helper botu komut listesine ulaşmak için \n"/help" komutuna tıklayınız.')


def gdu_command(update, context,ozet=0):
    reply = ""
    reply += backend.main("MASKED",ozet)
    reply = reply.replace("<br>", "\n")

    update.message.reply_text(reply)


def gdk_command(update, context,ozet=0):
    reply = ""
    reply += backend.main("MASKED ", ozet)
    reply = reply.replace("<br>", "\n")

    update.message.reply_text(reply)


def hdo_command(update, context,ozet=0):
    reply = ""
    reply += backend.main("MASKED", ozet)
    reply = reply.replace("<br>", "\n")

    update.message.reply_text(reply)


def hdk_command(update, context,ozet=0):
    reply = ""
    reply += backend.main("MASKED", ozet)
    reply = reply.replace("<br>", "\n")

    update.message.reply_text(reply)


def tanim_aktarimlari_command(update, context,ozet=0):
    reply = ""
    reply += backend.main("MASKED",ozet)
    reply = reply.replace("<br>", "\n")

    update.message.reply_text(reply)


def alt_ozetler_command(update, context,ozet=0):
    reply = ""
    reply += backend.main("MASKED", ozet)
    reply = reply.replace("<br>", "\n")

    update.message.reply_text(reply)

def ozet_command(update, context):
    ozet=1
    tanim_aktarimlari_command(update, context,ozet)
    alt_ozetler_command(update, context,ozet)
    gdu_command(update, context,ozet)
    gdk_command(update, context,ozet)
    hdo_command(update, context,ozet)
    hdk_command(update, context,ozet)
    ozet=0

def help_command(update, context):
    update.message.reply_text('Aşağıdaki komutları çalıştırabilirsiniz; \n /gdu \n  /tanim_aktarimlari \n  /alt_ozetler\n  /gdk\n  /hdo\n  /hdk\n  /ozet')








def main():


    updater = Updater(keys.consigliere_bot_telegram_token, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start_command))
    dp.add_handler(CommandHandler("help", help_command))
    dp.add_handler(CommandHandler("gdu", gdu_command))
    dp.add_handler(CommandHandler("tanim_aktarimlari", tanim_aktarimlari_command))
    dp.add_handler(CommandHandler("alt_ozetler", alt_ozetler_command))
    dp.add_handler(CommandHandler("gdk", gdk_command))
    dp.add_handler(CommandHandler("hdo", hdo_command))
    dp.add_handler(CommandHandler("hdk", hdk_command))
    dp.add_handler(CommandHandler("ozet", ozet_command))


    updater.start_polling()
    updater.idle()



main()


