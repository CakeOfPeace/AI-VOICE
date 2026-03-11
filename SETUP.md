# AI AVATAR — Deployment Guide

Full deployment instructions for a fresh Ubuntu 22.04/24.04 server.

---

## Architecture Overview

| Component | Description |
|-----------|-------------|
| **agent.py** | LiveKit voice agent — connects to LiveKit Cloud, handles inbound/outbound SIP calls, STT/LLM/TTS pipelines. Runs as a supervisor process that spawns per-number workers. |
| **config_api.py** | Flask REST API (port 5057) — agent CRUD, user auth, analytics, Twilio integration, embed widgets, provider management. Managed by PM2. |
| **PostgreSQL** | Primary data store (agents, calls, transcripts, analytics, credentials). |
| **PM2** | Process manager for both `config_api.py` and `agent.py`. |
| **Nginx** | Reverse proxy with SSL termination. |

Both `agent.py` and `config_api.py` share the same `.env`, storage backends, and database. The config API is the control plane that spawns and manages agent workers.

---

## 1. System Packages

```bash
sudo apt update && sudo apt upgrade -y

sudo apt install -y \
  build-essential \
  python3 python3-dev python3-venv python3-pip \
  libffi-dev libssl-dev \
  libsndfile1 ffmpeg \
  postgresql postgresql-contrib \
  nginx certbot python3-certbot-nginx \
  curl wget git unzip \
  ufw
```

Verify Python version (3.11+ required, 3.12 recommended):

```bash
python3 --version
```

---

## 2. Node.js & PM2

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
sudo npm install -g pm2
```

---

## 3. PostgreSQL Setup

```bash
sudo systemctl enable postgresql
sudo systemctl start postgresql
```

Create database and user:

```bash
sudo -u postgres psql <<'SQL'
CREATE USER livekit WITH PASSWORD 'livekit';
CREATE DATABASE livekit_agents OWNER livekit;
GRANT ALL PRIVILEGES ON DATABASE livekit_agents TO livekit;
\q
SQL
```

If your `.env` uses different credentials (e.g. `DB_USER=agent`, `DB_NAME=voice_agent`), adjust accordingly.

---

## 4. Clone / Transfer Project

```bash
# From GitHub
cd /root
git clone https://github.com/YOUR_USER/AI-AVATAR.git "AI AVATAR"

# Or from another server via rsync
rsync -avz --exclude='venv' --exclude='__pycache__' \
  root@SOURCE_IP:"/root/AI AVATAR/" "/root/AI AVATAR/"
```

---

## 5. Python Virtual Environment

```bash
cd "/root/AI AVATAR"
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 6. Environment Configuration

```bash
cp .env.example .env
nano .env
```

**Variables you MUST set:**

| Variable | Notes |
|----------|-------|
| `LIVEKIT_URL` | Your LiveKit Cloud project URL |
| `LIVEKIT_API_KEY` | LiveKit API key |
| `LIVEKIT_API_SECRET` | LiveKit API secret |
| `LIVEKIT_SIP_*` | SIP trunk config from LiveKit dashboard |
| `TWILIO_ACCOUNT_SID` | Twilio account SID |
| `TWILIO_AUTH_TOKEN` | Twilio auth token |
| `TWILIO_PHONE_NUMBER` | Your Twilio phone number |
| `OPENAI_API_KEY` | OpenAI API key |
| `GROQ_API_KEY` | Groq API key (optional) |
| `FISH_AUDIO_API_KEY` | Fish Audio API key |
| `DEFAULT_VOICE` | Fish Audio voice ID |
| `CREDENTIALS_ENCRYPTION_KEY` | Generate with command below |
| `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` | Must match PostgreSQL setup |
| `INTERNAL_API_KEY` | Shared secret for internal API calls |
| `LIVEKIT_RECORDING_S3_*` | S3-compatible storage for call recordings |
| `BEY_API_KEY` | Beyond Presence avatar API key (optional) |

**Generate an encryption key:**

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## 7. Database Migration

```bash
cd "/root/AI AVATAR"
source venv/bin/activate
python -m db.migrate
```

This creates the following tables:
- `kv_store` — key-value configuration storage
- `livekit_calls` — per-call records
- `call_transcripts` — call transcript entries
- `call_metrics` — derived analytics and AI analysis
- `agent_batch_analysis` — aggregated agent analysis
- `telephony_events` — telephony webhook log
- `scheduled_calls` — outbound call scheduler
- `callback_results` — external callback audit log

Verify:

```bash
sudo -u postgres psql -d livekit_agents -c "\dt"
```

---

## 8. Start Services

### Config API (via PM2)

```bash
cd "/root/AI AVATAR"
pm2 start ecosystem.config.cjs
pm2 save
pm2 startup
```

Verify:

```bash
pm2 status
curl -s http://localhost:5057/providers | python3 -m json.tool
```

### Agent Process (via PM2)

```bash
cd "/root/AI AVATAR"
pm2 start agent.py \
  --name "voice-agent" \
  --interpreter "./venv/bin/python" \
  --cwd "/root/AI AVATAR"

pm2 save
```

Check logs:

```bash
pm2 logs voice-agent --lines 50
pm2 logs config-api --lines 50
```

---

## 9. Nginx Reverse Proxy

```bash
sudo tee /etc/nginx/sites-available/ai-avatar <<'NGINX'
server {
    listen 80;
    server_name YOUR_DOMAIN;

    location / {
        proxy_pass http://127.0.0.1:5057;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
        proxy_connect_timeout 75s;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }

    client_max_body_size 50M;
}
NGINX

sudo ln -sf /etc/nginx/sites-available/ai-avatar /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl reload nginx
```

Replace `YOUR_DOMAIN` with your actual domain or server IP.

---

## 10. SSL with Let's Encrypt

```bash
sudo certbot --nginx -d YOUR_DOMAIN
sudo certbot renew --dry-run
```

---

## 11. Firewall (UFW)

```bash
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow ssh
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
```

---

## 12. Verification Checklist

```bash
# PostgreSQL running and tables exist
sudo systemctl status postgresql
sudo -u postgres psql -d livekit_agents -c "\dt"

# Config API responding
curl -s http://localhost:5057/providers | head -c 200

# PM2 processes online
pm2 status

# Agent connecting to LiveKit
pm2 logs voice-agent --lines 20

# Nginx proxying correctly
curl -s http://YOUR_DOMAIN/providers | head -c 200

# SSL active (after certbot)
curl -s https://YOUR_DOMAIN/providers | head -c 200
```

---

## Common Commands

```bash
# Live logs
pm2 logs --lines 100

# Restart services
pm2 restart config-api
pm2 restart voice-agent
pm2 restart all

# Stop everything
pm2 stop all

# Monitor CPU/memory
pm2 monit

# Database shell
sudo -u postgres psql -d livekit_agents

# Re-run migrations
cd "/root/AI AVATAR" && ./venv/bin/python -m db.migrate

# Check active calls
curl -s -H "X-Internal-Key: $(grep INTERNAL_API_KEY .env | cut -d= -f2)" \
  http://localhost:5057/livekit/calls | python3 -m json.tool
```

---

## Project Structure

```
AI AVATAR/
├── agent.py                  # LiveKit voice agent (supervisor + workers)
├── config_api.py             # Flask REST API (port 5057)
├── call_artifacts.py         # S3 recording + transcript upload
├── transcription_manager.py  # Per-call transcript management
├── fish_tts_provider.py      # Fish Audio TTS plugin
├── workflow_engine.py        # Workflow tool builder
├── encryption.py             # Fernet credential encryption
├── handoff.py                # Agent-to-agent & SIP warm transfer
├── requirements.txt          # Python dependencies
├── ecosystem.config.cjs      # PM2 config for config-api
├── .env.example              # Environment template (copy to .env)
├── SETUP.md                  # This file
├── storage/                  # Storage backend abstraction
│   ├── __init__.py           #   Factory (json or postgres)
│   ├── base.py               #   Abstract interface
│   ├── json_backend.py       #   JSON file backend (fallback)
│   └── postgres_backend.py   #   PostgreSQL backend (primary)
├── db/                       # Database layer
│   ├── __init__.py
│   ├── core.py               #   Engine & session factory
│   ├── models.py             #   SQLAlchemy models
│   └── migrate.py            #   Table creation
├── providers/                # STT/LLM/TTS/Realtime provider system
│   ├── __init__.py           #   Provider registry & catalog
│   ├── factory.py            #   Provider instantiation
│   ├── loader.py             #   Credential loading pipeline
│   ├── resolver.py           #   Credential resolution chain
│   ├── validators.py         #   API key validation
│   └── xai_realtime.py       #   xAI Grok realtime model
├── tools/                    # Agent callable tools
│   └── __init__.py
├── memory/                   # Agent memory (Mem0)
│   └── __init__.py
├── analytics/                # Call analytics & AI analysis
│   ├── __init__.py
│   └── service.py
├── integrations/             # Third-party integrations
│   ├── __init__.py           #   Auto-registry
│   ├── base.py               #   IntegrationBase
│   ├── cal_com.py            #   Cal.com
│   ├── google_calendar.py    #   Google Calendar
│   ├── google_sheets.py      #   Google Sheets
│   ├── airtable_data.py      #   Airtable
│   ├── hubspot_crm.py        #   HubSpot CRM
│   ├── slack_webhook.py      #   Slack
│   ├── twilio_sms.py         #   Twilio SMS
│   ├── sendgrid_email.py     #   SendGrid
│   ├── notion_data.py        #   Notion
│   └── generic_webhook.py    #   Generic HTTP webhook
├── embed/                    # Embeddable UI
│   ├── agents.html           #   Admin panel
│   └── widget.html           #   Voice widget
├── background_ambience/      # Background audio assets
├── agents/                   # Runtime JSON data (auto-created)
├── greetings/                # Generated greeting audio
├── recordings/               # Local recording storage
├── transcripts/              # Transcript JSON files
└── logs/                     # Application logs
```

---

## Troubleshooting

**Agent won't connect to LiveKit:**
- Verify `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET` in `.env`
- Check `pm2 logs voice-agent` for connection errors
- Ensure the server can reach `wss://*.livekit.cloud` (no firewall blocking outbound WSS)

**Config API returns 500:**
- Check `pm2 logs config-api` for stack traces
- Verify PostgreSQL is running: `sudo systemctl status postgresql`
- Test DB connection: `psql "postgresql://livekit:livekit@127.0.0.1:5432/livekit_agents"`

**Database migration fails:**
- Ensure PostgreSQL user has CREATE TABLE privileges
- Check `DB_*` env vars match your PostgreSQL setup

**No audio / TTS not working:**
- Verify `FISH_AUDIO_API_KEY` is valid
- Check `DEFAULT_VOICE` matches a valid Fish Audio voice ID
- Ensure `libsndfile1` and `ffmpeg` are installed

**Twilio calls not routing:**
- Verify `LIVEKIT_SIP_TRUNK_ID` matches your LiveKit SIP trunk
- Check `TWILIO_ACCOUNT_SID` and `TWILIO_AUTH_TOKEN`

**PM2 not starting after reboot:**
- Run `pm2 startup` and execute the command it prints
- Run `pm2 save` after all processes are running

**pip install fails:**
- Ensure `build-essential python3-dev libffi-dev libssl-dev` are installed
- Try: `pip install --no-cache-dir -r requirements.txt`
