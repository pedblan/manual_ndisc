from src.login_openai import login
from src.structured_outputs import schema
import json

client = login()

def analisar_figuras(discurso):
    response = client.responses.create(
      model="gpt-5",
      input=[
        {
          "role": "developer",
          "content": [
            {
              "type": "input_text",
              "text": "Você é um linguista que analisa figuras de linguagem em discursos no Senado."
            }
          ]
        },
        {
          "role": "user",
          "content": [
            {
              "type": "input_text",
              "text": f"Analise a seguinte fala:\n\n{discurso}"
            }
          ]
        }
      ],
      text={
        "format": schema,
        "verbosity": "medium"
      },
      reasoning={
        "effort": "medium"
      },
      tools=[],
      store=True
    )

    return response

def obter_resposta(response) -> list:
  """Retorna uma lista com as figuras analisadas de um discurso específico. """
  raw = response.output_text
  data = json.loads(raw)  # dict em Python
  spans = data["spans"]
  return spans