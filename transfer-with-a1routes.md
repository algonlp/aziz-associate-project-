# Transfer With A1Routes (Asterisk + Twilio SIP + ARI/AMI)

This note documents the exact fix we applied so transfers no longer drop and the caller is bridged to the transfer target correctly.

## Summary of the Root Cause

Transfers were failing because ARI/AMI was continuing the **Twilio leg** (PJSIP/twilio_in-XXXX) instead of the **A1Routes caller leg** (PJSIP/a1r-XXXX).  
When the Twilio leg is moved to the transfer context, Asterisk hangs up the original caller, then dials the transfer target from the wrong leg. This causes the original call to drop or be silent.

## Fix Overview

1) Always prefer the **A1Routes channel** when choosing the ARI channel to continue.  
2) Store the **A1Routes ARI channel ID** in the call context (not the Twilio channel).  
3) Allow overriding the preference with an env var.

## App Changes (Must Be Deployed)

### 1) Prefer A1Routes channel for transfer

File: `functions/transfer_call.py`

- When resolving the ARI channel (by CallSid or metadata), prefer channels named like `PJSIP/a1r-...`.
- A helper `_channel_name_matches_prefix()` is used.
- Adds env var:
  - `ASTERISK_TRANSFER_CHANNEL_PREFIX` (default: `PJSIP/a1r-`)

### 2) Store preferred ARI channel in ARI event listener

File: `app.py`

- The ARI listener now stores channel IDs **only if they match the preferred prefix** (or if no channel is stored yet).
- This ensures the call context uses the A1Routes caller leg.

### 3) Environment variables

Set in `.env`:
```
ASTERISK_TRANSFER_CHANNEL_PREFIX=PJSIP/a1r-
TRANSFER_NUMBER=+923190288141
DEFAULT_TRANSFER_NUMBER=+923190288141
```

Also set transfer number in app JSONs (used by the UI/agents):
- `prompt_cache.json` (per agent)
- `inbound_config.json`
- `transfernumbers.json`

## Asterisk Dialplan (Must Exist)

File: `/etc/asterisk/extensions.conf`

```
[ari-handoff]
exten => _X.,1,NoOp(ARI handoff)
 same => n,Stasis(ai-app)
 same => n,Hangup()

[call-transfer]
exten => _X.,1,NoOp(Transfer to ${EXTEN})
 same => n,Set(CALLERID(num)=912242580180)   ; A1Routes allowed CLI
 same => n,Dial(PJSIP/a1r/sip:10002799${EXTEN}@sip.a1routes.com,60)
 same => n,NoOp(Dialstatus=${DIALSTATUS} Hangupcause=${HANGUPCAUSE})
 same => n,Hangup()
```

Reload after change:
```
asterisk -rx "dialplan reload"
```

## Asterisk ARI/AMI Config (Required for redirect + continue)

### AMI (manager.conf)
- AMI must be enabled and allow the app server IP.
- Example:
```
[general]
enabled = yes
port = 5038

[ai]
secret = <PASSWORD>
read = all
write = all
permit = <APP_SERVER_IP>/255.255.255.255
```

Ensure Asterisk binds to `0.0.0.0:5038` if remote AMI is needed:
```
asterisk -rx "manager show settings"
```

### ARI
- ARI must be enabled and reachable from the app server (port 8088).
- The app listens to Stasis app name: `ai-app`.

## Asterisk PJSIP (A1Routes)

File: `/etc/asterisk/pjsip.conf`

Key settings for the A1Routes endpoint:
```
[a1r]
type=endpoint
transport=transport-udp
from_user=10002799
context=from_a1routes
disallow=all
allow=ulaw,alaw
aors=a1r_aor
outbound_auth=a1r_auth
from_domain=sip.a1routes.com
outbound_proxy=sip:18.215.199.86\;lr
rtp_symmetric=yes
force_rport=yes
rewrite_contact=yes
direct_media=no
timers=yes
rtp_timeout=30
rtp_timeout_hold=300
```

Reload:
```
asterisk -rx "pjsip reload"
```

## RTP + Firewall

### RTP range
`/etc/asterisk/rtp.conf`
```
[general]
rtpstart=10000
rtpend=20000
```

### Firewall (UFW)
Allow SIP and RTP from A1Routes and Twilio:
```
ufw allow 5060/udp from 18.215.199.86
ufw allow 10000:20000/udp from 18.215.199.86
ufw allow 5060/udp from 54.172.60.0/30
ufw allow 10000:20000/udp
ufw reload
```

A1Routes also uses:
```
18.210.7.0
3.232.240.36
```
Ensure UDP from those IPs is allowed on RTP range if your firewall is restricted.

## Diagnostics

### Verify dialplan
```
asterisk -rx "dialplan show call-transfer"
asterisk -rx "dialplan show ari-handoff"
```

### Watch live logs
```
tail -f /var/log/asterisk/full
```

### Check that the A1Routes leg is used in transfer
You should see:
```
... Executing [<number>@call-transfer:2] Dial(PJSIP/a1r/sip:10002799<number>@sip.a1routes.com,60)
```

### Quick sanity call (A1Routes leg)
```
asterisk -rx "channel originate PJSIP/a1r/sip:10002799923190288141@sip.a1routes.com application Playback hello-world callerid=912242580180"
```

## Expected Behavior After Fix

- Caller says “transfer”
- App finds A1Routes channel and continues it into `call-transfer`
- Asterisk dials the transfer target using A1Routes
- Caller stays connected and is bridged to the transfer target
- Call no longer drops immediately
