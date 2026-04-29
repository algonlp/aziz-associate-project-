from typing import List, Optional


class CallContext:
    """Store context for the current call."""
    def __init__(self):
        self.stream_sid: Optional[str] = None
        self.call_sid: Optional[str] = None
        self.ari_channel_id: Optional[str] = None
        self.call_ended: bool = False
        self.user_context: List = []
        self.system_message: str = ""
        self.twilio_from: Optional[str] = None
        self.twilio_to: Optional[str] = None
        self.dialed_number: Optional[str] = None
        self.start_time: Optional[str] = None
        self.end_time: Optional[str] = None
        self.final_status: Optional[str] = None
        self.voice = None  # Add the voice attribute
        self.language = None
        self.conversation_history: List = []
        self.agent_name: Optional[str] = None
        self.email_tool: Optional[str] = None
        self.email_recipient: Optional[str] = None
        self.agent_type: Optional[str] = None
        self.campaign_id: Optional[str] = None
        self.pricing_data: Optional[dict] = None
        self.transfer_offer_pending: bool = False
        self.transfer_offer_expires_at: Optional[float] = None
        self.transfer_user_confirmed: bool = False
        self.usage_intent: Optional[str] = None
        self.last_assistant_message: Optional[str] = None
        self.last_assistant_question: Optional[str] = None
        self.assistant_has_spoken: bool = False
        self.agent_id: Optional[str] = None
        self.last_tool_call_interaction: Optional[int] = None
        self.last_tool_call: Optional[dict] = None
        self.script_lines: List[str] = []
        self.script_index: int = 0
        self.tts_speaking: bool = False
        self.last_tts_start_ts: Optional[float] = None
        self.last_tts_stop_ts: Optional[float] = None
        self.last_tts_first_audio_ts: Optional[float] = None
        self.last_assistant_utterance_ts: Optional[float] = None
        self.last_user_final_ts: Optional[float] = None
        self.last_user_text: Optional[str] = None
        self.conversation_state: str = "CALL_START"
        self.state_machine_enabled: bool = True
        self.selected_configuration: Optional[str] = None
        self.intent_ack_sent: bool = False
        self.config_ack_sent: bool = False
        self.fast_intro_pending: bool = True
        self.intro_text: Optional[str] = None
        self.intro_sent: bool = False
        self.project_overview_text: Optional[str] = None
        self.project_overview_sentences: List[str] = []
        self.project_overview_index: int = 0
        self.closing_text: Optional[str] = None
        self.end_call_phrases: List[str] = []
        self.auto_end_scheduled: bool = False
        self.ending_in_progress: bool = False
        
