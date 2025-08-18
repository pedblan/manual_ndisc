import os
from openai import OpenAI

def login():
    """
    Retorna um cliente OpenAI autenticado a partir da variável de ambiente.

    Returns:
        OpenAI: Cliente autenticado da API OpenAI.
    """
    try:
        api_key = os.getenv("OPENAI_API_KEY")
    except:
        print("Configure a variável de ambiente OPENAI_API_KEY")

    return OpenAI(api_key=api_key)
