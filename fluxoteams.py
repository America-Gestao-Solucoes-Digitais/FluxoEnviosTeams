import os
import time
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

URL_WEBHOOK = os.getenv("URL_WEBHOOK")


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


def buscar_unidades_sem_emissao():
    """
    Busca todas as unidades ativas de ENERGIA e calcula quantos dias
    se passaram desde a última DATA_EMISSAO em tb_dfat_gestao_faturas_energia_novo.
    Retorna apenas unidades com mais de 35 dias sem emissão (ou sem nenhuma emissão).
    """
    conn = mysql.connector.connect(**DB_CONFIG)
    query = """
        SELECT
            c.INSTALACAO_MATRICULA,
            c.GRUPO,
            c.NOME_UNIDADE,
            MAX(f.DATA_EMISSAO)                          AS ULTIMA_EMISSAO,
            DATEDIFF(CURDATE(), MAX(f.DATA_EMISSAO))     AS DIAS_SEM_EMISSAO
        FROM tb_clientes_gestao_faturas AS c
        LEFT JOIN tb_dfat_gestao_faturas_energia_novo AS f
            ON c.INSTALACAO_MATRICULA = f.COD_INSTALACAO
        WHERE c.UTILIDADE = 'ENERGIA'
          AND c.STATUS_UNIDADE = 'Ativa'
        GROUP BY c.INSTALACAO_MATRICULA, c.GRUPO, c.NOME_UNIDADE
        HAVING DIAS_SEM_EMISSAO > 35 OR ULTIMA_EMISSAO IS NULL
        ORDER BY DIAS_SEM_EMISSAO DESC
    """
    df = pd.read_sql(query, conn)

    # Filtrando apenas os clientes MAGAZINE LUIZA, DASA, PERNAMBUCANAS, ABIJCSUD, RENNER, GRUPO MIME, PEPSICO, SANTANDER, MARISA e KORA
    df = df[df["GRUPO"].str.contains("MAGAZINE LUIZA|DASA|PERNAMBUCANAS|ABIJCSUD|RENNER|GRUPO MIME|PEPSICO|SANTANDER|MARISA|KORA", case=False, na=False)].reset_index(drop=True)

    # Deixando a coluna Dias sem emissao como inteiro (em vez de float)
    df["DIAS_SEM_EMISSAO"] = df["DIAS_SEM_EMISSAO"].fillna(0).astype(int)

    # Desconsiderando as datas de emissão vazias
    df = df[~df["ULTIMA_EMISSAO"].isna()].reset_index(drop=True)

    conn.close()
    return df


def buscar_vencimentos_amanha():
    """
    Busca as faturas com vencimento amanhã, fazendo JOIN entre:
      - tb_dfat_gestao_faturas_energia_novo  (coluna COD_INSTALACAO)
      - tb_clientes_gestao_faturas           (coluna INSTALACAO_MATRICULA)
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


def enviar_via_webhook(card_content):
    """Envia um Adaptive Card para o Teams via webhook (Power Automate)."""
    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": card_content
            }
        ]
    }
    resp = requests.post(
        URL_WEBHOOK,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=10
    )
    print(f"      Status: {resp.status_code}")
    if resp.status_code not in (200, 201, 202):
        print(f"      Resposta: {resp.text[:500]}")
    resp.raise_for_status()


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
        return [card_content]

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
        payloads.append(card_content)

    return payloads


def _linha_emissao(grupo, unidade, instalacao, ultima_emissao, dias, cabecalho=False):
    peso = "Bolder" if cabecalho else "Default"
    cor_dias = "Attention" if not cabecalho and isinstance(dias, (int, float)) and dias > 60 else "Default"
    return {
        "type": "ColumnSet",
        "columns": [
            {"type": "Column", "width": 3, "items": [{"type": "TextBlock", "text": str(grupo), "weight": peso, "wrap": True}]},
            {"type": "Column", "width": 4, "items": [{"type": "TextBlock", "text": str(unidade), "weight": peso, "wrap": True}]},
            {"type": "Column", "width": 2, "items": [{"type": "TextBlock", "text": str(instalacao), "weight": peso, "horizontalAlignment": "Center"}]},
            {"type": "Column", "width": 2, "items": [{"type": "TextBlock", "text": str(ultima_emissao), "weight": peso, "horizontalAlignment": "Center"}]},
            {"type": "Column", "width": 2, "items": [{"type": "TextBlock", "text": str(dias), "weight": peso, "color": cor_dias, "horizontalAlignment": "Right"}]},
        ]
    }


def montar_lotes_sem_emissao(df):
    hoje = datetime.now().strftime("%d/%m/%Y")
    total_unidades = len(df)
    payloads = []

    if df.empty:
        card_content = {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                {"type": "TextBlock", "text": f"Unidades sem emissao (> 35 dias) — {hoje}", "weight": "Bolder", "size": "Large", "color": "Accent"},
                {"type": "TextBlock", "text": "Nenhuma unidade encontrada.", "isSubtle": True}
            ]
        }
        return [card_content]

    total_lotes = (total_unidades + LOTE_TAMANHO - 1) // LOTE_TAMANHO

    for i, inicio in enumerate(range(0, total_unidades, LOTE_TAMANHO)):
        lote = df.iloc[inicio:inicio + LOTE_TAMANHO]
        num_lote = i + 1

        if num_lote == 1:
            titulo = f"Unidades sem emissao (> 35 dias) — {hoje}"
            subtitulo = f"{total_unidades} unidade(s) | Parte {num_lote}/{total_lotes}"
        else:
            titulo = f"Unidades sem emissao (> 35 dias) — {hoje} (continuacao {num_lote}/{total_lotes})"
            subtitulo = f"Unidades {inicio + 1} a {min(inicio + LOTE_TAMANHO, total_unidades)}"

        linhas = [_linha_emissao("Grupo", "Unidade", "Instalacao", "Ult. Emissao", "Dias", cabecalho=True)]
        primeira = True
        for _, row in lote.iterrows():
            ultima = row.get("ULTIMA_EMISSAO")
            if pd.isnull(ultima) or ultima is None:
                ultima_fmt = "Sem emissao"
                dias_fmt = "N/A"
            else:
                ultima_fmt = pd.to_datetime(ultima).strftime("%d/%m/%Y")
                dias_fmt = int(row.get("DIAS_SEM_EMISSAO", 0))

            linha = _linha_emissao(
                row.get("GRUPO", "-"),
                row.get("NOME_UNIDADE", "-"),
                row.get("INSTALACAO_MATRICULA", "-"),
                ultima_fmt,
                dias_fmt
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
                {"type": "TextBlock", "text": titulo, "weight": "Bolder", "size": "Large", "color": "Warning"},
                {"type": "TextBlock", "text": subtitulo, "isSubtle": True, "spacing": "None"},
                *linhas
            ]
        }
        payloads.append(card_content)

    return payloads


# ==========================================================
# EXECUÇÃO
# ==========================================================
def _enviar_lotes(lotes, descricao):
    print(f"\n   Enviando {descricao} ({len(lotes)} lote(s))...")
    for i, card in enumerate(lotes, 1):
        print(f"      Lote {i}/{len(lotes)}...")
        enviar_via_webhook(card)
        if i < len(lotes):
            time.sleep(1)
    print(f"      {descricao} enviado(s) com sucesso!")


def executar_fluxo():
    print("=" * 50)
    print("Iniciando fluxo")
    print("=" * 50)

    print("\n[1/4] Buscando vencimentos de amanha no banco...")
    df_vencimentos = buscar_vencimentos_amanha()
    print(f"      {len(df_vencimentos)} fatura(s) encontrada(s).")

    print("\n[2/4] Buscando unidades sem emissao no banco...")
    df_sem_emissao = buscar_unidades_sem_emissao()
    print(f"      {len(df_sem_emissao)} unidade(s) encontrada(s).")

    print("\n[3/4] Montando lotes...")
    lotes_vencimentos = montar_lotes(df_vencimentos)
    lotes_sem_emissao = montar_lotes_sem_emissao(df_sem_emissao)
    print(f"      Vencimentos: {len(lotes_vencimentos)} lote(s) | Sem emissao: {len(lotes_sem_emissao)} lote(s).")

    print("\n[4/4] Enviando para o Teams via Webhook...")
    _enviar_lotes(lotes_vencimentos, "Vencimentos de amanha")
    _enviar_lotes(lotes_sem_emissao, "Unidades sem emissao")

    print("\n" + "=" * 50)
    print("Fluxo concluido.")
    print("=" * 50)


if __name__ == "__main__":
    executar_fluxo()
