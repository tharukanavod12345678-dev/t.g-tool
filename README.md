# Telegram Pull — v15 (patched)

## Features
1. Progress auto-notifications
2. Failed video retry queue
3. Bandwidth tracking (/stats)
6. Multi-source support (/sources)
7. Dry-run mode (/dry-run)
9. /export command

## Install
chmod +x install.sh
./install.sh

## First Run Wizard (සිංහල)
Bot start → Saved Messages → wizard (ප්‍රශ්න 7).
Sources: comma එකෙන් channels කිහිපයක් දෙන්නත් පුළුවන්.

## Commands
Setup : /setup  /defaults  /config  /cancel
Info  : /status  /load  /stats
Config: /source  /sources add|rm|list|clear  /target
        /types  /maxmb  /minmb  /blacklist  /unblacklist  /quiet
Resrc : /throttle  /cap  /schedule
Ctrl  : /pause  /resume  /jump  /rescan  /stop
Tools : /dry-run on|off  /retry  /export

## Safety
nice 19 + ionice idle. CPU > 80% slow, > 90% pause. RAM cap 350 MB.
OOMScoreAdjust=500. PID lock. /stop clean exit.
