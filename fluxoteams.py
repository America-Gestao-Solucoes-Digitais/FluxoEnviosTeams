import os
import json
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
            c.GRUPO
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
LOTE_TAMANHO = 20


def _linha_tabela(grupo, instalacao, vencimento, valor, cabecalho=False):
    peso = "Bolder" if cabecalho else "Default"
    return {
        "type": "ColumnSet",
        "columns": [
            {"type": "Column", "width": 4, "items": [{"type": "TextBlock", "text": str(grupo), "weight": peso, "wrap": True}]},
            {"type": "Column", "width": 2, "items": [{"type": "TextBlock", "text": str(instalacao), "weight": peso, "horizontalAlignment": "Center"}]},
            {"type": "Column", "width": 2, "items": [{"type": "TextBlock", "text": str(vencimento), "weight": peso, "horizontalAlignment": "Center"}]},
            {"type": "Column", "width": 2, "items": [{"type": "TextBlock", "text": str(valor), "weight": peso, "horizontalAlignment": "Right"}]},
        ]
    }


def montar_lotes(df):
    amanha = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
    total_faturas = len(df)
    payloads = []

    if df.empty:
        card_content = {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                {"type": "TextBlock", "text": f"Vencimentos para amanha - {amanha}", "weight": "Bolder", "size": "Large", "color": "Accent"},
                {"type": "TextBlock", "text": "Nenhuma fatura encontrada.", "isSubtle": True}
            ]
        }
        return [{"message": json.dumps(card_content, ensure_ascii=False)}]

    total_geral = float(df["VALOR_TOTAL"].sum())
    total_fmt = f"R$ {total_geral:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    total_lotes = (total_faturas + LOTE_TAMANHO - 1) // LOTE_TAMANHO

    for i, inicio in enumerate(range(0, total_faturas, LOTE_TAMANHO)):
        lote = df.iloc[inicio:inicio + LOTE_TAMANHO]
        num_lote = i + 1

        if num_lote == 1:
            titulo = f"Vencimentos para amanha - {amanha}"
            subtitulo = f"{total_faturas} fatura(s) | Total: {total_fmt} | Parte {num_lote}/{total_lotes}"
        else:
            titulo = f"Vencimentos para amanha - {amanha} (continuacao {num_lote}/{total_lotes})"
            subtitulo = f"Faturas {inicio + 1} a {min(inicio + LOTE_TAMANHO, total_faturas)}"

        linhas = [_linha_tabela("Grupo", "Instalacao", "Vencimento", "Valor (R$)", cabecalho=True)]
        primeira = True
        for _, row in lote.iterrows():
            valor = float(row.get("VALOR_TOTAL", 0))
            valor_fmt = f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            linha = _linha_tabela(
                row.get("GRUPO", "-"),
                row.get("COD_INSTALACAO", "-"),
                amanha,
                valor_fmt
            )
            if primeira:
                linha["separator"] = True
                primeira = False
            linhas.append(linha)

        card_content = {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                {"type": "TextBlock", "text": titulo, "weight": "Bolder", "size": "Large", "color": "Accent"},
                {"type": "TextBlock", "text": subtitulo, "isSubtle": True, "spacing": "None"},
                *linhas
            ]
        }
        payloads.append({"message": json.dumps(card_content, ensure_ascii=False)})

    return payloads


def enviar_via_webhook(card):
    resp = requests.post(
        TEAMS_WEBHOOK_URL,
        data=json.dumps(card, ensure_ascii=False),
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

    print("\n[2/3] Montando lotes...")
    lotes = montar_lotes(df_vencimentos)
    print(f"      {len(lotes)} lote(s) de ate {LOTE_TAMANHO} faturas cada.")

    print("\n[3/3] Enviando para o Teams via Webhook...")
    for i, lote in enumerate(lotes, 1):
        print(f"      Enviando lote {i}/{len(lotes)}...")
        enviar_via_webhook(lote)
    print("      Todos os lotes enviados com sucesso!")

    print("\n" + "=" * 50)
    print("Fluxo concluído.")
    print("=" * 50)


if __name__ == "__main__":
    executar_fluxo()
