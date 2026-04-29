## Twilio Voice Call Limits

Summary from the Twilio Support article *"How many calls can Twilio handle?"*:

- **API rate limit:** Outbound calls created via the `Calls` REST API are limited to one request per second per Twilio account SID by default. Support can raise the rate limit after verifying your traffic profile.
- **Concurrent legs:** Each project can sustain hundreds of simultaneous call legs (100+ by default, up to ~1000 with approval). Hitting the API faster than the per-second rate simply yields HTTP 429 responses.
- **Practical maximums:** Twilio Support confirmed that most self-service projects can be raised to ~10 call creations per second with an approved traffic plan; higher bursts require enterprise review.

### How the app enforces this

- `TWILIO_CALL_THROTTLE_SEC` (default `1.0`) forces the backend to pause between `client.calls.create(...)` invocations so we stay within the documented REST rate limit.
- Bulk calls are queued sequentially so that even large CSV uploads respect the throttle and avoid 429 responses.
- The Streamlit bulk launcher surfaces `total_successful`/`total_failed` counts so you can quickly spot rate-limit issues and either slow the throttle or engage Twilio to raise CPS.

If you receive 429 errors despite the throttle, contact Twilio Support with your expected CPS (calls per second) and they can raise the limits for your project.
