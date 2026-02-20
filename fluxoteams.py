import os
import pandas as pd
import mysql.connector
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

# ==========================================================
# CONFIGURAÇÕES GERAIS
# ==========================================================
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL")


# ==========================================================
# BANCO DE DADOS
# ==========================================================
def buscar_unidades_gestao_faturas():
    conn = mysql.connector.connect(**DB_CONFIG)
    query = """
        SELECT * FROM tb_clientes_gestao_faturas
        WHERE UTILIDADE = 'ENERGIA'
        AND STATUS_UNIDADE = 'Ativa';
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def buscar_faturas_lidas_gestao_faturas():
    conn = mysql.connector.connect(**DB_CONFIG)
    query = """
        SELECT * FROM tb_dfat_gestao_faturas_energia_novo
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def buscar_vencimentos_amanha():
    """
    Busca as faturas com vencimento amanhã, fazendo JOIN entre:
      - tb_dfat_gestao_faturas_energia_novo  (coluna COD_INSTALACAO)
      - tb_clientes_gestao_faturas           (coluna INSTALACAO_MATRICULA)

    Ajuste os nomes das colunas se necessário:
      - VENCIMENTO   -> data de vencimento da fatura
      - VALOR        -> valor da fatura
      - NOME_UNIDADE -> nome da unidade/cliente
    """
    amanha = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
        SELECT
            f.COD_INSTALACAO,
            f.DATA_VENCIMENTO,
            f.VALOR_TOTAL,
            c.NOME_UNIDADE
        FROM tb_dfat_gestao_faturas_energia_novo AS f
        INNER JOIN tb_clientes_gestao_faturas AS c
            ON f.COD_INSTALACAO = c.INSTALACAO_MATRICULA
        WHERE DATE(f.DATA_VENCIMENTO) = '{amanha}'
          AND c.UTILIDADE = 'ENERGIA'
          AND c.STATUS_UNIDADE = 'Ativa'
        ORDER BY c.NOME_UNIDADE
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# ==========================================================
# MICROSOFT TEAMS — WEBHOOK
# ==========================================================
def montar_card_teams(df):
    """
    Monta um Adaptive Card formatado para envio via webhook.
    """
    amanha = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")

    if df.empty:
        facts = [{"title": "Resultado", "value": "Nenhum vencimento encontrado para amanhã."}]
        total_text = ""
    else:
        facts = []
        for _, row in df.iterrows():
            nome = str(row.get("NOME_UNIDADE", "—"))
            instalacao = str(row.get("COD_INSTALACAO", "—"))
            valor = float(row.get("VALOR", 0))
            valor_fmt = f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            facts.append({
                "title": nome,
                "value": f"Instalação: {instalacao} | Vencimento: {amanha} | {valor_fmt}"
            })

        total = float(df["VALOR"].sum())
        total_fmt = f"R$ {total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        total_text = f"**Total geral: {total_fmt}**"

    card = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": f"📅 Vencimentos para amanhã — {amanha}",
                            "weight": "Bolder",
                            "size": "Large",
                            "color": "Accent"
                        },
                        {
                            "type": "TextBlock",
                            "text": f"{len(df)} fatura(s) encontrada(s)",
                            "isSubtle": True,
                            "spacing": "None"
                        },
                        {
                            "type": "FactSet",
                            "facts": facts,
                            "spacing": "Medium"
                        },
                        *(
                            [{
                                "type": "TextBlock",
                                "text": total_text,
                                "weight": "Bolder",
                                "spacing": "Medium",
                                "separator": True
                            }] if total_text else []
                        )
                    ]
                }
            }
        ]
    }

    return card


def enviar_via_webhook(card):
    resp = requests.post(
        TEAMS_WEBHOOK_URL,
        json=card,
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    resp.raise_for_status()


# ==========================================================
# EXECUÇÃO
# ==========================================================
def executar_fluxo():
    print("=" * 50)
    print("Iniciando fluxo — Vencimentos do dia seguinte")
    print("=" * 50)

    print("\n[1/3] Buscando vencimentos de amanhã no banco...")
    df_vencimentos = buscar_vencimentos_amanha()
    print(f"      {len(df_vencimentos)} fatura(s) encontrada(s).")

    print("\n[2/3] Montando mensagem...")
    card = montar_card_teams(df_vencimentos)

    print("\n[3/3] Enviando para o Teams via Webhook...")
    enviar_via_webhook(card)
    print("      Mensagem enviada com sucesso!")

    print("\n" + "=" * 50)
    print("Fluxo concluído.")
    print("=" * 50)


if __name__ == "__main__":
    executar_fluxo()
