import json
from services.redis_client import get_redis_client

redis_client = get_redis_client()

def save_call_context(call_sid, call_context):
    """
    Save the call context to Redis.
    """
    context_dict = {
        "system_message": getattr(call_context, "system_message", None),
        "call_sid": getattr(call_context, "call_sid", None),
        "user_context": getattr(call_context, "user_context", None),
        "transfer_number": getattr(call_context, "transfer_number", None),
        "transfer_in_progress": getattr(call_context, "transfer_in_progress", None),
        "transfer_started": getattr(call_context, "transfer_started", None),
        "transfer_completed": getattr(call_context, "transfer_completed", None),
        "ari_channel_id": getattr(call_context, "ari_channel_id", None),
        "twilio_from": getattr(call_context, "twilio_from", None),
        "twilio_to": getattr(call_context, "twilio_to", None),
        "dialed_number": getattr(call_context, "dialed_number", None),
        "voice": getattr(call_context, "voice", None),
        "language": getattr(call_context, "language", None),
        "conversation_history": getattr(call_context, "conversation_history", None),
        "agent_name": getattr(call_context, "agent_name", None),
        "email_tool": getattr(call_context, "email_tool", None),
        "email_recipient": getattr(call_context, "email_recipient", None),
        "agent_type": getattr(call_context, "agent_type", None),
        "campaign_id": getattr(call_context, "campaign_id", None),
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
        "script_lines": getattr(call_context, "script_lines", None),
        "script_index": getattr(call_context, "script_index", None),
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
