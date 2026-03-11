module.exports = {
  apps: [{
    name: "config-api",
    script: "config_api.py",
    interpreter: "./venv/bin/python",
    cwd: "/root/AI AVATAR",
    env: {
      STORAGE_BACKEND: "postgres",
      DATABASE_URL: "postgresql+psycopg://agent:bM4vl3Ahcc0i2zGSBaqH@localhost:5432/voice_agent",
      CREDENTIALS_ENCRYPTION_KEY: "7zUDIa8lKAkNpJykAUQfgpVBx-s3t1-gM6gfF1ijrh4=",
      LIVEKIT_URL: "wss://zainlee-v566whmg.livekit.cloud",
      LIVEKIT_API_KEY: "API3h5XFGsYLpG2",
      LIVEKIT_API_SECRET: "rpRC4iu3gCfhbe1mjijES3QcrVXETLK25JGzHM2p7Ob",
      LIVEKIT_SIP_TRUNK_ID: "ST_YgKVDX3pgMeb"
    }
  }]
}
