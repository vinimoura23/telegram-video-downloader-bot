# Telegram Video Downloader Bot

Bot de Telegram para receber links de video e oferecer download em `MP3` ou `MP4` usando `yt-dlp`.

## Funcionalidades atuais

- Recebe links enviados por mensagem.
- Valida se a mensagem parece uma URL `http` ou `https`.
- Detecta se o link e um video unico ou uma playlist.
- Mostra botoes para baixar em `MP3` ou `MP4`.
- Usa `yt-dlp` para baixar o conteudo.
- Converte audio para `MP3` com `FFmpeg`.
- Envia playlists item por item no formato escolhido.
- Prioriza `MP4` em boa qualidade, preferindo video `mp4` com audio `m4a` quando disponivel.
- Envia o arquivo de volta no Telegram e limpa arquivos temporarios.
- Responde `/start`, `/help` e `ping`.

## Requisitos

- Python `3.8+`
- `ffmpeg` instalado no sistema para a opcao `MP3`
- Token de bot do Telegram

## Ambiente virtual

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuracao

Crie um arquivo `.env` na raiz do projeto:

```env
TELEGRAM_BOT_TOKEN=seu_token_aqui
```

## Execucao

```bash
source .venv/bin/activate
python telegram_bot.py
```

## Testes

```bash
source .venv/bin/activate
python -m unittest discover -s tests
```
