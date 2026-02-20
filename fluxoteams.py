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
def _celula(texto, negrito=False, alinhamento="Left"):
    return {
        "type": "TableCell",
        "items": [{
            "type": "TextBlock",
            "text": str(texto),
            "weight": "Bolder" if negrito else "Default",
            "horizontalAlignment": alinhamento,
            "wrap": False
        }]
    }


def montar_card_teams(df):
    """
    Monta um Adaptive Card com tabela para envio via webhook.
    """
    amanha = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")

    # Linha de cabeçalho
    cabecalho = {
        "type": "TableRow",
        "style": "accent",
        "cells": [
            _celula("Unidade", negrito=True),
            _celula("Instalação", negrito=True, alinhamento="Center"),
            _celula("Vencimento", negrito=True, alinhamento="Center"),
            _celula("Valor (R$)", negrito=True, alinhamento="Right"),
        ]
    }

    if df.empty:
        linhas_tabela = [{
            "type": "TableRow",
            "cells": [
                _celula("Nenhum vencimento encontrado para amanhã."),
                _celula("—", alinhamento="Center"),
                _celula("—", alinhamento="Center"),
                _celula("—", alinhamento="Right"),
            ]
        }]
        total_fmt = "R$ 0,00"
    else:
        linhas_tabela = []
        for _, row in df.iterrows():
            nome = str(row.get("NOME_UNIDADE", "—"))
            instalacao = str(row.get("COD_INSTALACAO", "—"))
            valor = float(row.get("VALOR_TOTAL", 0))
            valor_fmt = f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            linhas_tabela.append({
                "type": "TableRow",
                "cells": [
                    _celula(nome),
                    _celula(instalacao, alinhamento="Center"),
                    _celula(amanha, alinhamento="Center"),
                    _celula(valor_fmt, alinhamento="Right"),
                ]
            })

        total = float(df["VALOR_TOTAL"].sum())
        total_fmt = f"R$ {total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    card = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.5",
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
                            "type": "Table",
                            "gridStyle": "accent",
                            "firstRowAsHeader": True,
                            "showGridLines": True,
                            "spacing": "Medium",
                            "columns": [
                                {"width": 4},
                                {"width": 2},
                                {"width": 2},
                                {"width": 2},
                            ],
                            "rows": [cabecalho, *linhas_tabela]
                        },
                        {
                            "type": "TextBlock",
                            "text": f"**Total geral: {total_fmt}**",
                            "weight": "Bolder",
                            "horizontalAlignment": "Right",
                            "spacing": "Small",
                            "separator": True
                        }
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
    print(f"      Status: {resp.status_code}")
    print(f"      Resposta: {resp.text[:500]}")
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
