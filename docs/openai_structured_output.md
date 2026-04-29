# Structured Output from OpenAI

This project uses OpenAI function calling to obtain reliable, structured
responses from call transcripts. Function calling is preferred over free-form
JSON because the model is required to emit a schema-shaped payload.

## Recommended approach

1. **Define a single function schema** with strict types and `required` fields.
2. **Force a function call** using `function_call={"name": "your_function"}`.
3. **Set `temperature=0`** to reduce variance in the structured output.
4. **Parse `message.function_call.arguments`** and validate the fields before use.
5. **Fallback safely** by extracting a JSON block if the function call is missing.

## Why function calling

- The schema is enforced by the model, reducing malformed JSON.
- It cleanly separates free-form reasoning from structured payloads.
- It works with the existing `openai.ChatCompletion.create` API used by the app.

## Alternative (newer API)

Some OpenAI models also support `response_format` with JSON schema. If you
upgrade to the newer Responses API, you can enforce:

```
response_format = {
  "type": "json_schema",
  "json_schema": {...}
}
```

Function calling remains the most backward-compatible option with this codebase.
