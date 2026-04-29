import json
from services.redis_client import get_redis_client

redis_client = get_redis_client()

def save_call_context(call_sid, call_context):
    context_dict = {
        "system_message": getattr(call_context, "system_message", None),
        "call_sid": getattr(call_context, "call_sid", None),
        "campaign_id": getattr(call_context, "campaign_id", None),
        "lead_name": getattr(call_context, "lead_name", None),
        "user_context": getattr(call_context, "user_context", None),
        "transfer_number": getattr(call_context, "transfer_number", None),
        "transfer_in_progress": getattr(call_context, "transfer_in_progress", None),
        "transfer_started": getattr(call_context, "transfer_started", None),
        "transfer_completed": getattr(call_context, "transfer_completed", None),
        "final_status": getattr(call_context, "final_status", None),
        "created_at": getattr(call_context, "created_at", None),
        "completed_at": getattr(call_context, "completed_at", None),
        "ari_channel_id": getattr(call_context, "ari_channel_id", None),
        "twilio_from": getattr(call_context, "twilio_from", None),
        "twilio_to": getattr(call_context, "twilio_to", None),
        "dialed_number": getattr(call_context, "dialed_number", None),
        "voice": getattr(call_context, "voice", None),
        "language": getattr(call_context, "language", None),
        "conversation_history": getattr(call_context, "conversation_history", None),
        "pricing_data": getattr(call_context, "pricing_data", None),
        "transfer_offer_pending": getattr(call_context, "transfer_offer_pending", None),
        "transfer_offer_expires_at": getattr(call_context, "transfer_offer_expires_at", None),
        "transfer_user_confirmed": getattr(call_context, "transfer_user_confirmed", None),
        "model": getattr(call_context, "model", None),
        "usage_intent": getattr(call_context, "usage_intent", None),
        "selected_configuration": getattr(call_context, "selected_configuration", None),
        "intent_ack_sent": getattr(call_context, "intent_ack_sent", None),
        "config_ack_sent": getattr(call_context, "config_ack_sent", None),
        "last_user_text": getattr(call_context, "last_user_text", None),
        "last_assistant_message": getattr(call_context, "last_assistant_message", None),
        "last_assistant_question": getattr(call_context, "last_assistant_question", None),
        "agent_id": getattr(call_context, "agent_id", None),
        "last_tool_call_interaction": getattr(call_context, "last_tool_call_interaction", None),
        "last_tool_call": getattr(call_context, "last_tool_call", None),
        "initial_greeting_twi": getattr(call_context, "initial_greeting_twi", None),
        "human_speaks_first": getattr(call_context, "human_speaks_first", None),
        "latency_mode": getattr(call_context, "latency_mode", None),
        "fast_intro_pending": getattr(call_context, "fast_intro_pending", None),
        "intro_text": getattr(call_context, "intro_text", None),
        "intro_sent": getattr(call_context, "intro_sent", None),
        "project_overview_text": getattr(call_context, "project_overview_text", None),
        "project_overview_sentences": getattr(call_context, "project_overview_sentences", None),
        "project_overview_index": getattr(call_context, "project_overview_index", None),
        "closing_text": getattr(call_context, "closing_text", None),
        "end_call_phrases": getattr(call_context, "end_call_phrases", None),
        "conversation_state": getattr(call_context, "conversation_state", None),
        "state_machine_enabled": getattr(call_context, "state_machine_enabled", None),
    }
    redis_client.set(f"call_context:{call_sid}", json.dumps(context_dict))

def get_call_context(call_sid):
    data = redis_client.get(f"call_context:{call_sid}")
    if data:
        call_data = json.loads(data)
        from services.call_context import CallContext
        call_context = CallContext()
        setattr(call_context, "system_message", call_data.get("system_message"))
        setattr(call_context, "call_sid", call_data.get("call_sid"))
        setattr(call_context, "campaign_id", call_data.get("campaign_id"))
        setattr(call_context, "lead_name", call_data.get("lead_name"))
        setattr(call_context, "user_context", call_data.get("user_context"))
        setattr(call_context, "transfer_number", call_data.get("transfer_number"))
        setattr(call_context, "transfer_in_progress", call_data.get("transfer_in_progress"))
        setattr(call_context, "transfer_started", call_data.get("transfer_started"))
        setattr(call_context, "transfer_completed", call_data.get("transfer_completed"))
        setattr(call_context, "final_status", call_data.get("final_status"))
        setattr(call_context, "created_at", call_data.get("created_at"))
        setattr(call_context, "completed_at", call_data.get("completed_at"))
        setattr(call_context, "ari_channel_id", call_data.get("ari_channel_id"))
        setattr(call_context, "twilio_from", call_data.get("twilio_from"))
        setattr(call_context, "twilio_to", call_data.get("twilio_to"))
        setattr(call_context, "dialed_number", call_data.get("dialed_number"))
        setattr(call_context, "voice", call_data.get("voice"))
        setattr(call_context, "language", call_data.get("language"))
        setattr(call_context, "conversation_history", call_data.get("conversation_history") or [])
        setattr(call_context, "pricing_data", call_data.get("pricing_data"))
        setattr(call_context, "transfer_offer_pending", call_data.get("transfer_offer_pending"))
        setattr(call_context, "transfer_offer_expires_at", call_data.get("transfer_offer_expires_at"))
        setattr(call_context, "transfer_user_confirmed", call_data.get("transfer_user_confirmed"))
        setattr(call_context, "model", call_data.get("model"))
        setattr(call_context, "usage_intent", call_data.get("usage_intent"))
        setattr(call_context, "selected_configuration", call_data.get("selected_configuration"))
        setattr(call_context, "intent_ack_sent", call_data.get("intent_ack_sent"))
        setattr(call_context, "config_ack_sent", call_data.get("config_ack_sent"))
        setattr(call_context, "last_user_text", call_data.get("last_user_text"))
        setattr(call_context, "last_assistant_message", call_data.get("last_assistant_message"))
        setattr(call_context, "last_assistant_question", call_data.get("last_assistant_question"))
        setattr(call_context, "agent_id", call_data.get("agent_id"))
        setattr(call_context, "last_tool_call_interaction", call_data.get("last_tool_call_interaction"))
        setattr(call_context, "last_tool_call", call_data.get("last_tool_call"))
        setattr(call_context, "initial_greeting_twi", call_data.get("initial_greeting_twi"))
        setattr(call_context, "human_speaks_first", call_data.get("human_speaks_first"))
        setattr(call_context, "latency_mode", call_data.get("latency_mode"))
        setattr(call_context, "fast_intro_pending", call_data.get("fast_intro_pending"))
        setattr(call_context, "intro_text", call_data.get("intro_text"))
        setattr(call_context, "intro_sent", call_data.get("intro_sent"))
        setattr(call_context, "project_overview_text", call_data.get("project_overview_text"))
        setattr(call_context, "project_overview_sentences", call_data.get("project_overview_sentences") or [])
        setattr(call_context, "project_overview_index", call_data.get("project_overview_index") or 0)
        setattr(call_context, "closing_text", call_data.get("closing_text"))
        setattr(call_context, "end_call_phrases", call_data.get("end_call_phrases") or [])
        setattr(
            call_context,
            "conversation_state",
            call_data.get("conversation_state") or getattr(call_context, "conversation_state", None),
        )
        setattr(call_context, "state_machine_enabled", call_data.get("state_machine_enabled"))
        return call_context
    return None

def delete_call_context(call_sid):
    redis_client.delete(f"call_context:{call_sid}")
