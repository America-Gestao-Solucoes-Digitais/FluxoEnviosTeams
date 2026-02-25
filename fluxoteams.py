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

    # Filtrando apenas o cliente DASA
    df = df[df["GRUPO"].str.contains("DASA", case=False, na=False)].reset_index(drop=True)

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

    # Filtrando apenas o cliente DASA
    df = df[df["GRUPO"].str.contains("DASA", case=False, na=False)].reset_index(drop=True)
    conn.close()
    return df


# ==========================================================
# MICROSOFT TEAMS — WEBHOOK
# ==========================================================
def enviar_via_webhook(mensagem_html):
    """Envia mensagem HTML para o Teams via webhook (Power Automate)."""
    resp = requests.post(
        URL_WEBHOOK,
        json={"message": mensagem_html},
        headers={"Content-Type": "application/json"},
        timeout=10
    )
    print(f"   Status: {resp.status_code}")
    if resp.status_code not in (200, 201, 202):
        print(f"   Resposta: {resp.text[:500]}")
    resp.raise_for_status()


def montar_mensagem_html(df):
    """Monta tabela HTML com os vencimentos do dia seguinte."""
    amanha = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")

    if df.empty:
        return (
            f"<b>Vencimentos para amanha - {amanha}</b><br>"
            "Nenhuma fatura encontrada."
        )

    total_geral = float(df["VALOR_TOTAL"].sum())
    total_fmt = f"R$ {total_geral:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    total_faturas = len(df)

    linhas = ""
    for _, row in df.iterrows():
        valor = float(row.get("VALOR_TOTAL", 0))
        valor_fmt = f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        linhas += (
            f"<tr>"
            f"<td>{row.get('GRUPO', '-')}</td>"
            f"<td>{row.get('COD_INSTALACAO', '-')}</td>"
            f"<td>{amanha}</td>"
            f"<td>{valor_fmt}</td>"
            f"</tr>"
        )

    return (
        f"<b>Vencimentos para amanha - {amanha}</b><br>"
        f"{total_faturas} fatura(s) &nbsp;|&nbsp; Total: {total_fmt}<br><br>"
        f"<table>"
        f"<tr><th>Grupo</th><th>Instalacao</th><th>Vencimento</th><th>Valor (R$)</th></tr>"
        f"{linhas}"
        f"</table>"
    )


# ==========================================================
# EXECUÇÃO
# ==========================================================
def executar_fluxo():
    print("=" * 50)
    print("Iniciando fluxo")
    print("=" * 50)

    print("\n[1/3] Buscando vencimentos de amanha no banco...")
    df_vencimentos = buscar_vencimentos_amanha()
    print(f"      {len(df_vencimentos)} fatura(s) encontrada(s).")

    print("\n[2/3] Montando mensagem HTML...")
    mensagem = montar_mensagem_html(df_vencimentos)

    print("\n[3/3] Enviando para o Teams via Webhook...")
    enviar_via_webhook(mensagem)
    print("      Enviado com sucesso!")

    print("\n" + "=" * 50)
    print("Fluxo concluido.")
    print("=" * 50)


if __name__ == "__main__":
    executar_fluxo()
