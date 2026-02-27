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

CHUNK_SIZE = 30  # Máximo de linhas por mensagem (evita RequestEntityTooLarge)

GRUPOS_EXCLUIDOS = (
    "GPA", "OI", "ENEL X GD", "VENANCIO", "CLVB",
    "BRADESCO", "TELEFONICA", "GBZEnergia", "GDS"
)

GESTORES_POR_GRUPO = {
    "ABIJCSUD":       ["guilherme.garcia@voraenergia.com.br", "wanderson.santos@voraenergia.com.br"],
    "DASA":           ["bruno.petrillo@voraenergia.com.br", "sabrina.gomes@voraenergia.com.br"],
    "MAGAZINE LUIZA": ["guilherme.garcia@voraenergia.com.br"],
    "MARISA":         ["gustavo.felix@voraenergia.com.br"],
    "PERNAMBUCANAS":  ["caio.augusto@voraenergia.com.br"],
    "RENNER":         ["caio.augusto@voraenergia.com.br"],
    "PEPSICO":        ["samuel.santos@voraenergia.com.br"],
    "SANTANDER":      ["samuel.santos@voraenergia.com.br"],
    "ZARA":           ["gustavo.felix@voraenergia.com.br"],
    "KORA":           ["guilherme.viana@voraenergia.com.br"],
}


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
    Exclui grupos da lista GRUPOS_EXCLUIDOS e grupos com GRUPO NULL.
    """
    excluidos = ", ".join(f"'{g}'" for g in GRUPOS_EXCLUIDOS)
    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
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
          AND c.GRUPO IS NOT NULL
          AND c.GRUPO NOT IN ({excluidos})
        GROUP BY c.INSTALACAO_MATRICULA, c.GRUPO, c.NOME_UNIDADE
        HAVING DIAS_SEM_EMISSAO > 35 OR ULTIMA_EMISSAO IS NULL
        ORDER BY DIAS_SEM_EMISSAO DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def buscar_vencimentos_amanha():
    """
    Busca as faturas com vencimento amanhã, fazendo JOIN entre:
      - tb_dfat_gestao_faturas_energia_novo  (coluna COD_INSTALACAO)
      - tb_clientes_gestao_faturas           (coluna INSTALACAO_MATRICULA)
    """
    amanha = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    excluidos = ", ".join(f"'{g}'" for g in GRUPOS_EXCLUIDOS)

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
          AND c.GRUPO IS NOT NULL
          AND c.GRUPO NOT IN ({excluidos})
        ORDER BY c.GRUPO, c.NOME_UNIDADE
    """
    df = pd.read_sql(query, conn)

    conn.close()
    return df


# ==========================================================
# MICROSOFT TEAMS — WEBHOOK
# ==========================================================
def linha_gestores_html(grupo):
    """Retorna linha HTML com os gestores do grupo para marcar na mensagem."""
    emails = GESTORES_POR_GRUPO.get(grupo, [])
    if not emails:
        return ""
    mencoes = ", ".join(f"@{e}" for e in emails)
    return f"<b>Gestores:</b> {mencoes}<br><br>"


def enviar_via_webhook(mensagem_html, grupo):
    """Envia mensagem HTML para o Teams via webhook (Power Automate).
    Passa 'grupo', 'message' e 'gestores' para o fluxo rotear e mencionar os responsáveis."""
    gestores = ";".join(GESTORES_POR_GRUPO.get(grupo, []))
    resp = requests.post(
        URL_WEBHOOK,
        json={"grupo": grupo, "message": mensagem_html, "gestores": gestores},
        headers={"Content-Type": "application/json"},
        timeout=10
    )
    print(f"   Status: {resp.status_code}")
    if resp.status_code not in (200, 201, 202):
        print(f"   Resposta: {resp.text[:500]}")
    resp.raise_for_status()


def montar_mensagem_html_emissao(df, grupo, parte=1, total=1):
    """Monta tabela HTML com unidades com emissão atrasada (>35 dias)."""
    if df.empty:
        return None

    linhas = ""
    for _, row in df.iterrows():
        ultima = row.get("ULTIMA_EMISSAO")
        ultima_fmt = pd.Timestamp(ultima).strftime("%d/%m/%Y") if pd.notna(ultima) else "Sem emissao"
        dias = row.get("DIAS_SEM_EMISSAO", "-")
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('INSTALACAO_MATRICULA', '-')}</td>"
            f"<td>{ultima_fmt}</td>"
            f"<td>{dias}</td>"
            f"</tr>"
        )

    parte_txt = f" (Parte {parte}/{total})" if total > 1 else ""
    return (
        f"{linha_gestores_html(grupo)}"
        f"<b>Unidades sem emissao (&gt;35 dias){parte_txt}</b><br>"
        f"{len(df)} unidade(s) com atraso<br><br>"
        f"<table>"
        f"<tr><th>Unidade</th><th>Instalacao</th><th>Ultima Emissao</th><th>Dias</th></tr>"
        f"{linhas}"
        f"</table>"
    )


def montar_mensagem_html(df, grupo, parte=1, total=1):
    """Monta tabela HTML com os vencimentos do dia seguinte."""
    amanha = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")

    if df.empty:
        return (
            f"{linha_gestores_html(grupo)}"
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

    parte_txt = f" (Parte {parte}/{total})" if total > 1 else ""
    return (
        f"{linha_gestores_html(grupo)}"
        f"<b>Vencimentos para amanha - {amanha}{parte_txt}</b><br>"
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

    print("\n[1/4] Buscando vencimentos de amanha...")
    df_vencimentos = buscar_vencimentos_amanha()
    print(f"      {len(df_vencimentos)} fatura(s) encontrada(s).")

    print("\n[2/4] Enviando vencimentos por grupo...")
    for grupo in df_vencimentos["GRUPO"].unique():
        df_grupo = df_vencimentos[df_vencimentos["GRUPO"] == grupo].reset_index(drop=True)
        chunks = [df_grupo.iloc[i:i+CHUNK_SIZE] for i in range(0, len(df_grupo), CHUNK_SIZE)]
        total = len(chunks)
        print(f"\n   Grupo: {grupo} ({len(df_grupo)} fatura(s), {total} parte(s))")
        for parte, chunk in enumerate(chunks, 1):
            enviar_via_webhook(montar_mensagem_html(chunk, grupo, parte, total), grupo)
            print(f"   Parte {parte}/{total} enviada com sucesso!")

    print("\n[3/4] Buscando unidades com emissao atrasada...")
    df_emissao = buscar_unidades_sem_emissao()
    print(f"      {len(df_emissao)} unidade(s) com atraso encontrada(s).")

    print("\n[4/4] Enviando emissoes atrasadas por grupo...")
    for grupo in df_emissao["GRUPO"].unique():
        df_grupo = df_emissao[df_emissao["GRUPO"] == grupo].reset_index(drop=True)
        chunks = [df_grupo.iloc[i:i+CHUNK_SIZE] for i in range(0, len(df_grupo), CHUNK_SIZE)]
        total = len(chunks)
        print(f"\n   Grupo: {grupo} ({len(df_grupo)} unidade(s), {total} parte(s))")
        for parte, chunk in enumerate(chunks, 1):
            mensagem = montar_mensagem_html_emissao(chunk, grupo, parte, total)
            if mensagem:
                enviar_via_webhook(mensagem, grupo)
                print(f"   Parte {parte}/{total} enviada com sucesso!")

    print("\n" + "=" * 50)
    print("Fluxo concluido.")
    print("=" * 50)


if __name__ == "__main__":
    executar_fluxo()
