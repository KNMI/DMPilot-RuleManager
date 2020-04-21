JSON_RULE_SCHEMA = {
    "id": "rule-engine-schema",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "JSON Schema for Rule Engine rules",
    "description": "Schema that validates rules.json",
    "type": "object",
    "patternProperties": {
        "^.*$": {
            "description": "Rule",
            "$ref": "#/definitions/rule",
            "format": "object"
        }
    },
    "definitions": {
        "rule": {
            "type": "object",
            "properties": {
                "function_name": {"type": "string"},
                "options": {"type": "object"},
                "conditions": {
                        "type": "array",
                        "$ref": "#/definitions/condition"
                },
                "timeout": {"type": "integer"},
                "description": {"type": "string"}
            },
            "required": ["function_name", "options", "conditions"],
            "additionalProperties": False
        },
        "condition": {
            "type": "array",
            "items": {
                    "type": "object",
                    "properties": {
                        "function_name": {"type": "string"},
                        "options": {"type": "object"}
                    },
                "required": ["function_name", "options"],
                "additionalProperties": False
            }
        }
    }
}
