# Workflow proposto (de ponta a ponta)

## 0) Objetivo & taxonomia (defina antes de codar)

- **Unidade de análise:** span textual (trecho) dentro do discurso.  
- **Rótulos principais** (enum inicial, adaptável ao Senado):  
  metáfora, metonímia, hipérbole, ironia, antítese, paradoxo, anáfora (repetição no início de frases), aliteração (som inicial repetido), eufemismo, gradação, prosopopeia, pergunta_retórica, citação_de_autoridade, apelo_popular, analogia, paródia, sarcasmo.  
- Comece com ~10–12 rótulos; refine depois do primeiro round.  
- **Saídas por span:**  
  - `label`  
  - `start_char`  
  - `end_char`  
  - `text`  
  - `rationale` (breve justificativa)  
  - `cues` (pistas como “comparativo”, “hipérbole numérica”)  
  - `confidence [0..1]`  

> 💡 Dica: para *pergunta retórica* e *anáfora*, dá para combinar heurísticas simples (pré-filtro) + LLM.

---

## 1) Preparação dos dados (SQLite → chunks com offsets)

- **Seleção:** pegue discursos com  
  ```sql
  Data BETWEEN '2007-01-01' AND '2024-12-31'
  ```
- **Chunking (token-aware):** janelas de ~1.5k–2k tokens com overlap (200–250 tokens).  
  Mantenha:
  - `chunk_id`  
  - `speech_id`  
  - `start_char_global`  
  - `end_char_global`  

- **Pré-filtros opcionais (baratinhos):**
  - Pergunta retórica → frases com `?` e marcadores (“obviamente…”, “por acaso…”).  
  - Anáfora → 2+ frases consecutivas começando com a mesma locução.  
  - Aliteração → 4+ palavras próximas com mesma inicial.  

Esses *flags* entram como **features no prompt**.

---

## 2) Structured Outputs – desenhe o JSON Schema

Você força o modelo a devolver JSON no formato que você precisa.  

Esqueleto resumido:

```python
SCHEMA = {
  "name": "figuras_linguagem",
  "schema": {
    "type": "object",
    "properties": {
      "spans": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "label": {"type": "string", "enum": [
              "metafora","metonimia","hiperbole","ironia","antitese",
              "paradoxo","anafora","aliteracao","eufemismo","gradacao",
              "prosopopeia","pergunta_retórica","citacao_de_autoridade",
              "apelo_popular","analogia","parodia","sarcasmo"
            ]},
            "start_char": {"type": "integer", "minimum": 0},
            "end_char": {"type": "integer", "minimum": 0},
            "text": {"type": "string"},
            "rationale": {"type": "string"},
            "cues": {"type": "array", "items": {"type": "string"}},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1}
          },
          "required": ["label","start_char","end_char","text","confidence"]
        }
      }
    },
    "required": ["spans"],
    "additionalProperties": False
  },
  "strict": True
}
```

---

## 3) Prompt (PT-BR, com pistas e “não-alucinar”)

- **Sistema:** papel do modelo (“você é um linguista…”) + definições concisas de cada rótulo.  
- **Instruções:**
  - “Extraia apenas spans com alta evidência; se não houver, retorne `spans: []`.”  
  - “Use offsets no texto do input abaixo (caracteres).”  
  - “Evite duplicatas (mesmo trecho com rótulos diferentes só se de fato couber).”  
  - “Adote confidence conservador.”  

- **Few-shot:** 2–4 exemplos curtos em português, input/output conforme o schema.  
- **Pré-filtros** entram como metadados:  
  ```json
  "flags": {"anafora_detectada": true, ...}
  ```

---

## 4) Chamada à API (dev/local) com Structured Outputs

Pseudocódigo minimalista:

```python
from openai import OpenAI
client = OpenAI()

resp = client.responses.create(
    model="gpt-4o-mini",  # compatível com Structured Outputs
    input=[{
        "role": "system", "content": SISTEMA_DEFINICOES
    },{
        "role": "user",
        "content": [
            {"type":"text", "text": f"[META] speech_id={sid}, chunk_id={cid}, start={start_char_global}"},
            {"type":"text", "text": TEXTO_DO_CHUNK}
        ]
    }],
    response_format={"type": "json_schema","json_schema": SCHEMA},
    temperature=0.0
)

data = resp.output[0].content[0].json
```

---

## 5) Escala: Batch API

- Gere `.jsonl` (uma requisição por linha).  
- Upload → criação do job → baixar output/error file.  
- Reconciliar offsets locais do chunk → globais do discurso.  

---

## 6) Avaliação: OpenAI Evals (+ golden set)

- **Golden set:** 150–300 spans em ~50 discursos.  
- **Métricas:**
  - Precisão / Recall / F1 por rótulo (IoU ≥ 0.5).  
  - Exact label match.  
  - Compliance ao schema.  

- **Grader LLM (opcional):** verificar se um span realmente expressa a figura.

---

## 7) Pós-processamento e entrega

- Normalizar spans duplicados (por overlap).  
- Persistir em tabela `Figuras (SQLite)`:
  ```
  id, speech_id, ano, label, start_char, end_char, text, rationale, confidence
  ```
- Agregações:
  - por ano e partido  
  - por tipo de sessão  
  - densidade por mil palavras  

- Visualização: séries temporais, facets, ranking de senadores/temas.  

---

## Esqueleto de código

### A) Chunking com offsets
```python
def chunk_text(texto: str, max_chars: int = 6000, overlap: int = 800):
    chunks, i, n = [], 0, len(texto)
    while i < n:
        start, end = i, min(n, i + max_chars)
        chunks.append((start, end, texto[start:end]))
        i = end - overlap if end - overlap > i else end
    return chunks
```

### B) Geração de linhas para Batch (.jsonl)
```python
import json

def make_batch_lines(model, schema, prompt_sys, speech_id, chunk_id, text, meta):
    user_content = [
        {"type":"text","text": f"[META] {json.dumps(meta, ensure_ascii=False)}"},
        {"type":"text","text": text}
    ]
    body = {
        "model": model,
        "input": [
            {"role":"system","content": prompt_sys},
            {"role":"user","content": user_content}
        ],
        "response_format": {"type":"json_schema","json_schema": schema},
        "temperature": 0.0
    }
    return {
        "custom_id": f"{speech_id}:{chunk_id}",
        "method": "POST",
        "url": "/v1/responses",
        "body": body
    }

def write_jsonl(lines, path):
    with open(path, "w", encoding="utf-8") as f:
        for ln in lines:
            f.write(json.dumps(ln, ensure_ascii=False) + "\n")
```

### C) Reconciliação de offsets
```python
def remap_offsets(spans, chunk_start):
    for s in spans:
        s["start_char"] += chunk_start
        s["end_char"]   += chunk_start
    return spans
```

### D) Métrica IoU para spans
```python
def iou_span(a, b):
    inter = max(0, min(a["end_char"], b["end_char"]) - max(a["start_char"], b["start_char"]))
    uni = (a["end_char"] - a["start_char"]) + (b["end_char"] - b["start_char"]) - inter
    return inter / uni if uni else 0.0
```

---

## Boas práticas & pegadinhas

- Modelos compatíveis com **Structured Outputs**.  
- Evitar “forçar” alucinações → permitir `spans: []`.  
- Produção: `temperature=0`.  
- Rodar Evals antes de escalar.  
- Batch: agrupar por tamanho similar, monitorar `error file`.  

---

## Como começar em 1 dia (plano relâmpago)

1. Definir rótulos + 4 exemplos few-shot.  
2. Criar schema e rodar 50 chunks via Responses.  
3. Medir tempo/custo.  
4. Montar golden set (~50 trechos rotulados).  
5. Subir Batch com 2–5k chunks.  
6. Rodar Evals e reportar P/R/F1.  

---

## Referências oficiais

- [Structured Outputs](https://platform.openai.com)  
- [Text generation](https://platform.openai.com)  
- [Responses API](https://platform.openai.com)  
- [Batch API](https://platform.openai.com)  
- [Evals](https://platform.openai.com)
