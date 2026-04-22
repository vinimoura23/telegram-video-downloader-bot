# Telegram Video Downloader Bot

Bot de Telegram para baixar audio em `MP3` ou video em `MP4` usando `yt-dlp`.

## O que ele faz

- recebe um link por mensagem;
- detecta se o link e um video unico ou uma playlist;
- para video unico, oferece `MP3`, `MP4` e outras qualidades;
- para playlist, baixa e envia item por item;
- pode rodar na API publica do Telegram ou com Bot API local.

## Requisitos

- Python 3.8+
- `ffmpeg`
- token do bot no Telegram
- Docker com `docker compose` se quiser usar servidor local

## Instalar

```bash
git clone git@github.com:vinimoura23/telegram-video-downloader-bot.git
cd telegram-video-downloader-bot
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configurar

Crie o `.env` a partir do exemplo:

```bash
cp .env.example .env
```

Preencha pelo menos:

```env
TELEGRAM_BOT_TOKEN=seu_token_aqui
```

## Modos de execucao

### 1. Modo simples

Usa a API publica do Telegram.

Limite pratico de upload: `49 MB`.

No `.env`:

```env
TELEGRAM_LOCAL_SERVER_ENABLED=0
```

### 2. Modo local

Usa o servidor oficial Bot API local do Telegram, iniciado junto com o bot.

Limite pratico de upload: ate `2000 MB`.

No `.env`:

```env
TELEGRAM_API_ID=seu_api_id
TELEGRAM_API_HASH=seu_api_hash
TELEGRAM_LOCAL_SERVER_ENABLED=1
TELEGRAM_LOCAL_API_URL=http://127.0.0.1:8081
TELEGRAM_LOCAL_SERVER_TIMEOUT=180
```

Notas:

- na primeira execucao o Docker pode demorar alguns minutos para compilar o servidor local;
- nas proximas execucoes ele reutiliza a imagem ja criada;
- se quiser forcar rebuild: `TELEGRAM_LOCAL_SERVER_FORCE_REBUILD=1`.

## Rodar

```bash
source .venv/bin/activate
python telegram_bot.py
```

## Testar

```bash
source .venv/bin/activate
python -m unittest discover -s tests
```

## Uso

No Telegram:

1. envie um link;
2. escolha `MP3` ou `MP4`;
3. se for video unico, use os botoes de qualidade quando quiser.

## Observacoes

- `logs/bot.log` e usado apenas para diagnostico local;
- o bot limpa arquivos temporarios apos cada envio;
- playlist pode demorar mais porque cada item e processado separadamente.
