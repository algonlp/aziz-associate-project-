from twilio.rest import Client
import os

account_sid = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
auth_token = os.getenv("TWILIO_AUTH_TOKEN", "").strip()

if not account_sid or not auth_token:
    raise RuntimeError("TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN must be set")

client = Client(account_sid, auth_token)

balance = client.api.balance.fetch()
print("Balance:", balance.balance)
print("Currency:", balance.currency)
