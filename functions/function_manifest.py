# functions/function_manifest.py
tools = [
    # Existing tools
    {
        "type": "function",
        "function": {
            "name": "transfer_call",
            "description": "Transfer the live call to a human consultant after explicit user consent.",
            "parameters": {
                "type": "object",
                "properties": {
                    "reason": {
                        "type": "string",
                        "description": "Short reason for transfer."
                    },
                    "transfer_number": {
                        "type": "string",
                        "description": "Optional transfer destination. If omitted, use the agent/admin configured transfer number."
                    }
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "end_call",
            "description": "End the current call immediately after the assistant has spoken a short closing sentence.",
            "parameters": {
                "type": "object",
                "properties": {
                    "reason": {
                        "type": "string",
                        "description": "Short reason for ending the call."
                    },
                    "farewell": {
                        "type": "string",
                        "description": "Optional final closing sentence already spoken to the user."
                    }
                },
                "required": []
            }
        }
    }
]
