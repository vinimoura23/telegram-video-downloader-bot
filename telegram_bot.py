import telebot
import yt_dlp
import logging
import os
import time
from telebot import types

bot = telebot.TeleBot('Seu bot ID aqui')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

user_data = {}

@bot.message_handler(func=lambda message: message.text.lower() == "ping")
def ping(message):
    bot.reply_to(message, "pong")

@bot.message_handler(func=lambda message: True)
def handle_message(message):
    try:
        if message.text.lower() == "ping":
            return

        link = message.text

        if not link.startswith(("http://", "https://")):
            bot.send_message(message.chat.id, "Por favor, envie um link válido.")
            return

        user_data[message.chat.id] = link

        markup = types.InlineKeyboardMarkup()
        btn_mp3 = types.InlineKeyboardButton("Baixar MP3", callback_data="mp3")
        btn_mp4 = types.InlineKeyboardButton("Baixar MP4", callback_data="mp4")
        markup.add(btn_mp3, btn_mp4)

        bot.send_message(message.chat.id, "Escolha o formato:", reply_markup=markup)

    except Exception as e:
        logging.error(f"Erro ao processar o link: {e}")
        bot.send_message(message.chat.id, "Ocorreu um erro ao processar o link.")

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    try:
        chat_id = call.message.chat.id
        link = user_data.get(chat_id)

        if not link:
            bot.send_message(chat_id, "Link não encontrado. Por favor, envie o link novamente.")
            return

        if call.data == "mp3":
            ydl_opts = {
                'format': 'bestaudio/best',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
                'outtmpl': '%(title)s.%(ext)s',
                'keepvideo': True,  
            }

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(link, download=True)
                filename = ydl.prepare_filename(info)
                filename_mp3 = filename.replace('.webm', '.mp3').replace('.m4a', '.mp3')

            with open(filename_mp3, 'rb') as audio_file:
                bot.send_audio(chat_id, audio_file)

            os.remove(filename_mp3)
            if os.path.exists(filename):  
                os.remove(filename)

        elif call.data == "mp4":
            ydl_opts = {
                'format': 'best',
                'outtmpl': '%(title)s.%(ext)s',
            }

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(link, download=True)
                filename = ydl.prepare_filename(info)

            with open(filename, 'rb') as video_file:
                bot.send_video(chat_id, video_file)

            os.remove(filename)

        if chat_id in user_data:
            del user_data[chat_id]

    except Exception as e:
        logging.error(f"Erro ao processar a escolha: {e}")
        bot.send_message(chat_id, "Ocorreu um erro ao processar sua escolha.")

bot.polling()