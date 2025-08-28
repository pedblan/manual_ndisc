schema = {
      "type": "json_schema",
      "name": "FigurasLinguagem",
      "strict": True,
      "schema": {
        "type": "object",
        "properties": {
          "spans": {
            "type": "array",
            "title": "Spans",
            "items": {
              "$ref": "#/$defs/Span"
            }
          }
        },
        "required": [
          "spans"
        ],
        "additionalProperties": False,
        "$defs": {
          "Span": {
            "type": "object",
            "title": "Span",
            "properties": {
              "label": {
                "type": "string",
                "title": "Label",
                "enum": [
                  "metafora",
                  "metonimia",
                  "hiperbole",
                  "ironia",
                  "antitese",
                  "paradoxo",
                  "anafora",
                  "aliteracao",
                  "eufemismo",
                  "gradacao",
                  "prosopopeia",
                  "pergunta_retórica",
                  "apelo_popular",
                  "analogia",
                  "assonancia",
                  "pleonasmo"
                ]
              },
              "start_char": {
                "type": "integer",
                "title": "Start Char",
                "minimum": 0
              },
              "end_char": {
                "type": "integer",
                "title": "End Char",
                "minimum": 0
              },
              "text": {
                "type": "string",
                "title": "Text"
              },
              "rationale": {
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "type": "null"
                  }
                ],
                "default": None,
                "title": "Rationale"
              },
              "cues": {
                "type": "array",
                "title": "Cues",
                "items": {
                  "type": "string"
                },
                "default": []
              },
              "confidence": {
                "type": "number",
                "title": "Confidence",
                "minimum": 0,
                "maximum": 1
              }
            },
            "required": [
              "label",
              "start_char",
              "end_char",
              "text",
              "rationale",
              "cues",
              "confidence"
            ],
            "additionalProperties": False
          }
        }
      }
    }
