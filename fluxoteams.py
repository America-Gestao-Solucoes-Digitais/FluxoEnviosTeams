import os
import warnings
import argparse
import smtplib
import pandas as pd
import mysql.connector
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

# Cores ANSI — azul=energia, ciano=água, verde=sucesso, amarelo=aviso
class C:
    R = "\033[0m";  B = "\033[1m"
    VERDE   = "\033[32m";  AMARELO = "\033[33m"
    AZUL    = "\033[34m";  CIANO   = "\033[36m"
    CINZA   = "\033[90m";  VERM    = "\033[31m"


# ==========================================================
# CONFIGURAÇÕES
# ==========================================================
DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

URL_WEBHOOK = os.getenv("URL_WEBHOOK")

SMTP_CONFIG = {
    "host":     os.getenv("SMTP_HOST", "smtp.office365.com"),
    "port":     int(os.getenv("SMTP_PORT", 587)),
    "user":     os.getenv("SMTP_USER"),
    "password": os.getenv("SMTP_PASS"),
}

EMAIL_REMETENTE     = os.getenv("SMTP_USER")
EMAIL_TESTE         = "guilherme.garcia@voraenergia.com.br"
EMAILS_CC           = ["caio.augusto@voraenergia.com.br", "pedro.queiroz@voraenergia.com.br"]
PERC_ALERTA         = 60   # % limiar de variação de valor total
PERC_ALERTA_CONSUMO = 30   # % limiar de variação de consumo
CHUNK_SIZE          = 50   # linhas por lote no fallback 413 (EntityTooLarge)
DIAS_TOLERANCIA     = 3    # janela de leitura (hoje − N dias) para TIMESTAMP/LOG

GRUPOS_EXCLUIDOS = (
    "GPA", "OI", "ENEL X GD", "VENANCIO", "CVLB",
    "BRADESCO", "TELEFONICA", "GBZEnergia", "GDS", "LIVRE ACL", "DROGAL", "REDE AMERICAS", "INTEGRADA"
)

# grupo → gestores {email, nome}
# "gestores" (emails;sep) e "gestores_nomes" (nomes;sep) são usados pelo Power Automate
# para buscar IDs no Azure AD e montar as menções @Nome no Teams.
GESTORES_POR_GRUPO = {
    "ABIJCSUD":       [
        {"email": "guilherme.garcia@voraenergia.com.br", "nome": "Guilherme Abdul"},
        {"email": "wanderson.santos@voraenergia.com.br", "nome": "Wanderson Santos"},
    ],
    "DASA":           [
        {"email": "bruno.petrillo@voraenergia.com.br",   "nome": "Bruno Petrillo"},
        {"email": "sabrina.gomes@voraenergia.com.br",    "nome": "Sabrina Gomes"},
    ],
    "MAGAZINE LUIZA": [{"email": "guilherme.garcia@voraenergia.com.br", "nome": "Guilherme Abdul"}],
    "MARISA":         [{"email": "gustavo.felix@voraenergia.com.br",    "nome": "Gustavo Felix"}],
    "PERNAMBUCANAS":  [{"email": "caio.augusto@voraenergia.com.br",     "nome": "Caio Augusto"}],
    "RENNER":         [{"email": "caio.augusto@voraenergia.com.br",     "nome": "Caio Augusto"}],
    "PEPSICO":        [{"email": "samuel.santos@voraenergia.com.br",    "nome": "Samuel Santos"}],
    "SANTANDER":      [{"email": "samuel.santos@voraenergia.com.br",    "nome": "Samuel Santos"}],
    "ZARA":           [{"email": "gustavo.felix@voraenergia.com.br",    "nome": "Gustavo Felix"}],
    "GRUPO MIME":     [{"email": "caio.augusto@voraenergia.com.br",     "nome": "Caio Augusto"}],
    "KORA":           [{"email": "guilherme.viana@voraenergia.com.br",  "nome": "Guilherme Viana"}],
}

GESTORES_POR_GRUPO_AGUA = {
    "DASA":           [{"email": "aline.granadier@voraenergia.com.br", "nome": "Aline Granadier"}],
    "REDE AMERICAS":  [{"email": "aline.granadier@voraenergia.com.br", "nome": "Aline Granadier"}],
    "MAGAZINE LUIZA": [{"email": "aline.granadier@voraenergia.com.br", "nome": "Aline Granadier"}],
}

CANAL_TEAMS_AGUA = {
    "DASA":           "DASA-AGUA",
    "REDE AMERICAS":  "REDEAMERICAS-AGUA",
    "MAGAZINE LUIZA": "MAGAZINE LUIZA-AGUA",
}


# ==========================================================
# HELPERS — GESTORES
# ==========================================================
def _gestores_lista(grupo, agua=False):
    d = GESTORES_POR_GRUPO_AGUA if agua else GESTORES_POR_GRUPO
    return d.get(grupo, [])

def emails_gestores(grupo):      return [g["email"] for g in _gestores_lista(grupo)]
def nomes_gestores(grupo):       return [g["nome"]  for g in _gestores_lista(grupo)]
def emails_gestores_agua(grupo): return [g["email"] for g in _gestores_lista(grupo, agua=True)]
def nomes_gestores_agua(grupo):  return [g["nome"]  for g in _gestores_lista(grupo, agua=True)]

def linha_gestores_html(grupo, agua=False):
    gestores = _gestores_lista(grupo, agua)
    if not gestores:
        return ""
    mencoes = ", ".join(f'<at>{g["nome"]}</at>' for g in gestores)
    return f"<b>Gestores:</b> {mencoes}<br><br>"

def linha_gestores_html_agua(grupo): return linha_gestores_html(grupo, agua=True)


# ==========================================================
# BANCO DE DADOS
# ==========================================================
def buscar_unidades_sem_emissao():
    """Unidades de energia ativas com mais de 50 dias sem emissão."""
    excluidos = ", ".join(f"'{g}'" for g in GRUPOS_EXCLUIDOS)
    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
        SELECT
            c.INSTALACAO_MATRICULA,
            c.GRUPO,
            c.NOME_UNIDADE,
            c.DISTRIBUIDORA,
            MAX(f.DATA_EMISSAO)                      AS ULTIMA_EMISSAO,
            DATEDIFF(CURDATE(), MAX(f.DATA_EMISSAO)) AS DIAS_SEM_EMISSAO
        FROM tb_clientes_gestao_faturas AS c
        LEFT JOIN tb_dfat_gestao_faturas_energia_novo AS f
            ON c.INSTALACAO_MATRICULA = f.COD_INSTALACAO
        WHERE c.UTILIDADE = 'ENERGIA'
          AND c.STATUS_UNIDADE <> 'Inativa'
          AND c.GRUPO IS NOT NULL
          AND c.GRUPO NOT IN ({excluidos})
        GROUP BY c.INSTALACAO_MATRICULA, c.GRUPO, c.NOME_UNIDADE, c.DISTRIBUIDORA
        HAVING DIAS_SEM_EMISSAO > 50 OR ULTIMA_EMISSAO IS NULL
        ORDER BY DIAS_SEM_EMISSAO DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def buscar_vencimentos_amanha():
    """Faturas de energia com vencimento amanhã."""
    amanha = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    excluidos = ", ".join(f"'{g}'" for g in GRUPOS_EXCLUIDOS)
    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
        SELECT
            f.COD_INSTALACAO,
            f.DATA_VENCIMENTO,
            f.VALOR_TOTAL,
            c.GRUPO,
            c.DISTRIBUIDORA
        FROM tb_dfat_gestao_faturas_energia_novo AS f
        INNER JOIN tb_clientes_gestao_faturas AS c
            ON f.COD_INSTALACAO = c.INSTALACAO_MATRICULA
        WHERE DATE(f.DATA_VENCIMENTO) = '{amanha}'
          AND c.UTILIDADE = 'ENERGIA'
          AND c.STATUS_UNIDADE <> 'Inativa'
          AND c.GRUPO IS NOT NULL
          AND c.GRUPO NOT IN ({excluidos})
        ORDER BY c.GRUPO, c.NOME_UNIDADE
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def buscar_variacao_consumo_agua():
    """Variação de consumo de água vs. A-1, janela de DIAS_TOLERANCIA dias."""
    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
        SELECT
            a.MATRICULA,
            c.GRUPO,
            c.NOME_UNIDADE,
            c.DISTRIBUIDORA,
            DATE_FORMAT(a.REFERENCIA, '%Y%m')                             AS REF_ATUAL,
            DATE_FORMAT(DATE_SUB(a.REFERENCIA, INTERVAL 1 YEAR), '%Y%m') AS REF_ANTERIOR,
            a.CONSUMO                                                     AS CONSUMO_ATUAL,
            ant.CONSUMO                                                   AS CONSUMO_ANT,
            ROUND(((a.CONSUMO - ant.CONSUMO) / NULLIF(ant.CONSUMO, 0)) * 100, 1) AS PERC_CONSUMO
        FROM tb_dfat_gestao_faturas_agua AS a
        INNER JOIN (
            SELECT MATRICULA, MAX(REFERENCIA) AS MAX_REF
            FROM tb_dfat_gestao_faturas_agua
            WHERE DATE(LOG) >= CURDATE() - INTERVAL {DIAS_TOLERANCIA} DAY
            GROUP BY MATRICULA
        ) AS ult ON a.MATRICULA = ult.MATRICULA AND a.REFERENCIA = ult.MAX_REF
        INNER JOIN tb_dfat_gestao_faturas_agua AS ant
            ON a.MATRICULA = ant.MATRICULA
           AND ant.REFERENCIA = DATE_SUB(a.REFERENCIA, INTERVAL 1 YEAR)
        INNER JOIN tb_clientes_gestao_faturas AS c
            ON a.MATRICULA = c.INSTALACAO_MATRICULA
        WHERE c.UTILIDADE = 'AGUA'
          AND c.STATUS_UNIDADE <> 'Inativa'
          AND c.GRUPO IS NOT NULL
          AND DATE(a.LOG) >= CURDATE() - INTERVAL {DIAS_TOLERANCIA} DAY
        HAVING PERC_CONSUMO > {PERC_ALERTA_CONSUMO}
        ORDER BY c.GRUPO, c.NOME_UNIDADE
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def buscar_variacao_valor_agua():
    """Variação de valor total de água vs. A-1, janela de DIAS_TOLERANCIA dias."""
    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
        SELECT
            a.MATRICULA,
            c.GRUPO,
            c.NOME_UNIDADE,
            c.DISTRIBUIDORA,
            DATE_FORMAT(a.REFERENCIA, '%Y%m')                             AS REF_ATUAL,
            DATE_FORMAT(DATE_SUB(a.REFERENCIA, INTERVAL 1 YEAR), '%Y%m') AS REF_ANTERIOR,
            a.TOTAL                                                       AS VALOR_ATUAL,
            ant.TOTAL                                                     AS VALOR_ANT,
            ROUND(((a.TOTAL - ant.TOTAL) / NULLIF(ant.TOTAL, 0)) * 100, 1) AS PERC_VALOR
        FROM tb_dfat_gestao_faturas_agua AS a
        INNER JOIN (
            SELECT MATRICULA, MAX(REFERENCIA) AS MAX_REF
            FROM tb_dfat_gestao_faturas_agua
            WHERE DATE(LOG) >= CURDATE() - INTERVAL {DIAS_TOLERANCIA} DAY
            GROUP BY MATRICULA
        ) AS ult ON a.MATRICULA = ult.MATRICULA AND a.REFERENCIA = ult.MAX_REF
        INNER JOIN tb_dfat_gestao_faturas_agua AS ant
            ON a.MATRICULA = ant.MATRICULA
           AND ant.REFERENCIA = DATE_SUB(a.REFERENCIA, INTERVAL 1 YEAR)
        INNER JOIN tb_clientes_gestao_faturas AS c
            ON a.MATRICULA = c.INSTALACAO_MATRICULA
        WHERE c.UTILIDADE = 'AGUA'
          AND c.STATUS_UNIDADE <> 'Inativa'
          AND c.GRUPO IS NOT NULL
          AND DATE(a.LOG) >= CURDATE() - INTERVAL {DIAS_TOLERANCIA} DAY
        HAVING PERC_VALOR > {PERC_ALERTA}
        ORDER BY c.GRUPO, c.NOME_UNIDADE
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def buscar_vencimentos_agua():
    """Faturas de água com vencimento amanhã."""
    amanha = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
        SELECT
            f.MATRICULA,
            f.VENCIMENTO,
            f.TOTAL,
            c.GRUPO,
            c.NOME_UNIDADE,
            c.DISTRIBUIDORA
        FROM tb_dfat_gestao_faturas_agua AS f
        INNER JOIN tb_clientes_gestao_faturas AS c
            ON f.MATRICULA = c.INSTALACAO_MATRICULA
        WHERE DATE(f.VENCIMENTO) = '{amanha}'
          AND c.UTILIDADE = 'AGUA'
          AND c.STATUS_UNIDADE <> 'Inativa'
          AND c.GRUPO IS NOT NULL
        ORDER BY c.GRUPO, c.NOME_UNIDADE
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def buscar_variacao_consumo():
    """Variação de consumo de energia vs. A-1, janela de DIAS_TOLERANCIA dias."""
    excluidos = ", ".join(f"'{g}'" for g in GRUPOS_EXCLUIDOS)
    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
        SELECT
            a.COD_INSTALACAO,
            c.GRUPO,
            c.NOME_UNIDADE,
            a.DISTRIBUIDORA,
            DATE_FORMAT(a.REFERENCIA, '%Y%m')                             AS REF_ATUAL,
            DATE_FORMAT(DATE_SUB(a.REFERENCIA, INTERVAL 1 YEAR), '%Y%m') AS REF_ANTERIOR,
            (a.CONSUMO_LIDO_FP + a.CONSUMO_LIDO_P)                       AS CONSUMO_ATUAL,
            (ant.CONSUMO_LIDO_FP + ant.CONSUMO_LIDO_P)                   AS CONSUMO_ANT,
            ROUND(
                (((a.CONSUMO_LIDO_FP + a.CONSUMO_LIDO_P) - (ant.CONSUMO_LIDO_FP + ant.CONSUMO_LIDO_P))
                / NULLIF((ant.CONSUMO_LIDO_FP + ant.CONSUMO_LIDO_P), 0)) * 100, 1
            ) AS PERC_CONSUMO
        FROM tb_dfat_gestao_faturas_energia_novo AS a
        INNER JOIN (
            SELECT COD_INSTALACAO, MAX(REFERENCIA) AS MAX_REF
            FROM tb_dfat_gestao_faturas_energia_novo
            WHERE DATE(TIMESTAMP) >= CURDATE() - INTERVAL {DIAS_TOLERANCIA} DAY
            GROUP BY COD_INSTALACAO
        ) AS ult ON a.COD_INSTALACAO = ult.COD_INSTALACAO AND a.REFERENCIA = ult.MAX_REF
        INNER JOIN tb_dfat_gestao_faturas_energia_novo AS ant
            ON a.COD_INSTALACAO = ant.COD_INSTALACAO
           AND ant.REFERENCIA = DATE_SUB(a.REFERENCIA, INTERVAL 1 YEAR)
        INNER JOIN tb_clientes_gestao_faturas AS c
            ON a.COD_INSTALACAO = c.INSTALACAO_MATRICULA
        WHERE c.UTILIDADE = 'ENERGIA'
          AND c.STATUS_UNIDADE <> 'Inativa'
          AND c.GRUPO IS NOT NULL
          AND c.GRUPO NOT IN ({excluidos})
          AND DATE(a.TIMESTAMP) >= CURDATE() - INTERVAL {DIAS_TOLERANCIA} DAY
        HAVING PERC_CONSUMO > {PERC_ALERTA_CONSUMO}
        ORDER BY c.GRUPO, c.NOME_UNIDADE
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def buscar_variacao_valor():
    """Variação de valor total de energia vs. A-1, janela de DIAS_TOLERANCIA dias."""
    excluidos = ", ".join(f"'{g}'" for g in GRUPOS_EXCLUIDOS)
    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
        SELECT
            a.COD_INSTALACAO,
            c.GRUPO,
            c.NOME_UNIDADE,
            a.DISTRIBUIDORA,
            DATE_FORMAT(a.REFERENCIA, '%Y%m')                             AS REF_ATUAL,
            DATE_FORMAT(DATE_SUB(a.REFERENCIA, INTERVAL 1 YEAR), '%Y%m') AS REF_ANTERIOR,
            a.VALOR_TOTAL                                                 AS VALOR_ATUAL,
            ant.VALOR_TOTAL                                               AS VALOR_ANT,
            ROUND(((a.VALOR_TOTAL - ant.VALOR_TOTAL) / NULLIF(ant.VALOR_TOTAL, 0)) * 100, 1) AS PERC_VALOR
        FROM tb_dfat_gestao_faturas_energia_novo AS a
        INNER JOIN (
            SELECT COD_INSTALACAO, MAX(REFERENCIA) AS MAX_REF
            FROM tb_dfat_gestao_faturas_energia_novo
            WHERE DATE(TIMESTAMP) >= CURDATE() - INTERVAL {DIAS_TOLERANCIA} DAY
            GROUP BY COD_INSTALACAO
        ) AS ult ON a.COD_INSTALACAO = ult.COD_INSTALACAO AND a.REFERENCIA = ult.MAX_REF
        INNER JOIN tb_dfat_gestao_faturas_energia_novo AS ant
            ON a.COD_INSTALACAO = ant.COD_INSTALACAO
           AND ant.REFERENCIA = DATE_SUB(a.REFERENCIA, INTERVAL 1 YEAR)
        INNER JOIN tb_clientes_gestao_faturas AS c
            ON a.COD_INSTALACAO = c.INSTALACAO_MATRICULA
        WHERE c.UTILIDADE = 'ENERGIA'
          AND c.STATUS_UNIDADE <> 'Inativa'
          AND c.GRUPO IS NOT NULL
          AND c.GRUPO NOT IN ({excluidos})
          AND DATE(a.TIMESTAMP) >= CURDATE() - INTERVAL {DIAS_TOLERANCIA} DAY
        HAVING PERC_VALOR > {PERC_ALERTA}
        ORDER BY c.GRUPO, c.NOME_UNIDADE
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# ==========================================================
# MICROSOFT TEAMS — WEBHOOK
# ==========================================================
def enviar_via_webhook(mensagem_html, grupo):
    """Envia mensagem ao Teams via Power Automate (canal energia)."""
    resp = requests.post(
        URL_WEBHOOK,
        json={
            "grupo":          grupo,
            "message":        mensagem_html,
            "gestores":       ";".join(emails_gestores(grupo)),
            "gestores_nomes": ";".join(nomes_gestores(grupo)),
        },
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    print(f"   {C.CINZA}Status: {resp.status_code}{C.R}")
    if resp.status_code not in (200, 201, 202):
        print(f"   {C.VERM}Resposta: {resp.text[:500]}{C.R}")
    resp.raise_for_status()


def enviar_via_webhook_agua(mensagem_html, grupo):
    """Envia mensagem ao Teams via Power Automate (canal água)."""
    canal = CANAL_TEAMS_AGUA.get(grupo, grupo)
    resp = requests.post(
        URL_WEBHOOK,
        json={
            "grupo":          canal,
            "message":        mensagem_html,
            "gestores":       ";".join(emails_gestores_agua(grupo)),
            "gestores_nomes": ";".join(nomes_gestores_agua(grupo)),
        },
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    print(f"   {C.CINZA}Status: {resp.status_code}{C.R}")
    if resp.status_code not in (200, 201, 202):
        print(f"   {C.VERM}Resposta: {resp.text[:500]}{C.R}")
    resp.raise_for_status()


def enviar_grupo_com_chunks(df, grupo, montar_fn):
    """Envia ao Teams; se 413 (EntityTooLarge), divide em lotes de CHUNK_SIZE."""
    mensagem = montar_fn(df, grupo)
    if not mensagem:
        return
    try:
        enviar_via_webhook(mensagem, grupo)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 413:
            total_lotes = (len(df) - 1) // CHUNK_SIZE + 1
            print(f"   {C.AMARELO}EntityTooLarge — enviando em {total_lotes} lote(s)...{C.R}")
            for i in range(0, len(df), CHUNK_SIZE):
                chunk = df.iloc[i:i + CHUNK_SIZE].reset_index(drop=True)
                msg = montar_fn(chunk, grupo)
                if msg:
                    enviar_via_webhook(msg, grupo)
                    print(f"   Lote {i // CHUNK_SIZE + 1}/{total_lotes} enviado.")
        else:
            raise


# ==========================================================
# HELPERS — FORMATAÇÃO
# ==========================================================
def _brl(valor):
    """Formata float como R$ 1.234,56."""
    return f"R$ {float(valor or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def montar_mensagem_html_emissao(df, grupo):
    if df.empty:
        return None
    linhas = ""
    for _, row in df.iterrows():
        ultima = row.get("ULTIMA_EMISSAO")
        ultima_fmt = pd.Timestamp(ultima).strftime("%d/%m/%Y") if pd.notna(ultima) else "Sem emissao"
        dias_raw = row.get("DIAS_SEM_EMISSAO")
        dias = int(dias_raw) if pd.notna(dias_raw) else "-"
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('INSTALACAO_MATRICULA', '-')}</td>"
            f"<td>{row.get('DISTRIBUIDORA', '-')}</td>"
            f"<td>{ultima_fmt}</td>"
            f"<td>{dias}</td>"
            f"</tr>"
        )
    return (
        f"{linha_gestores_html(grupo)}"
        f"<b>Unidades sem emissao (&gt;50 dias)</b><br>"
        f"{len(df)} unidade(s) com atraso<br><br>"
        f"<table>"
        f"<tr><th>Unidade</th><th>Instalacao</th><th>Distribuidora</th><th>Ultima Emissao</th><th>Dias</th></tr>"
        f"{linhas}</table>"
    )


def montar_mensagem_html(df, grupo):
    amanha = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
    if df.empty:
        return (
            f"{linha_gestores_html(grupo)}"
            f"<b>Vencimentos para amanha - {amanha}</b><br>"
            "Nenhuma fatura encontrada."
        )
    linhas = ""
    for _, row in df.iterrows():
        linhas += (
            f"<tr>"
            f"<td>{row.get('GRUPO', '-')}</td>"
            f"<td>{row.get('COD_INSTALACAO', '-')}</td>"
            f"<td>{row.get('DISTRIBUIDORA', '-')}</td>"
            f"<td>{amanha}</td>"
            f"<td>{_brl(row.get('VALOR_TOTAL', 0))}</td>"
            f"</tr>"
        )
    return (
        f"{linha_gestores_html(grupo)}"
        f"<b>Vencimentos para amanha - {amanha}</b><br>"
        f"{len(df)} fatura(s) &nbsp;|&nbsp; Total: {_brl(df['VALOR_TOTAL'].sum())}<br><br>"
        f"<table>"
        f"<tr><th>Grupo</th><th>Instalacao</th><th>Distribuidora</th><th>Vencimento</th><th>Valor (R$)</th></tr>"
        f"{linhas}</table>"
    )


def montar_mensagem_html_consumo(df, grupo):
    if df.empty:
        return None
    linhas = ""
    for _, row in df.iterrows():
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('COD_INSTALACAO', '-')}</td>"
            f"<td>{row.get('DISTRIBUIDORA', '-')}</td>"
            f"<td>{row.get('REF_ATUAL', '-')}</td>"
            f"<td>{row.get('REF_ANTERIOR', '-')}</td>"
            f"<td>{row.get('CONSUMO_ATUAL', '-')}</td>"
            f"<td>{row.get('CONSUMO_ANT', '-')}</td>"
            f"<td>{row.get('PERC_CONSUMO', '-')}%</td>"
            f"</tr>"
        )
    return (
        f"{linha_gestores_html(grupo)}"
        f"<b>Alerta de Consumo - Aumento &gt;{PERC_ALERTA_CONSUMO}% (vs. A-1)</b><br>"
        f"{len(df)} unidade(s) com variacao relevante<br><br>"
        f"<table><tr>"
        f"<th>Unidade</th><th>Instalacao</th><th>Distribuidora</th>"
        f"<th>Ref. Atual</th><th>Ref. A-1</th>"
        f"<th>Consumo Atual (kWh)</th><th>Consumo A-1 (kWh)</th><th>% Variacao</th>"
        f"</tr>{linhas}</table>"
    )


def montar_mensagem_html_valor(df, grupo):
    if df.empty:
        return None
    linhas = ""
    for _, row in df.iterrows():
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('COD_INSTALACAO', '-')}</td>"
            f"<td>{row.get('DISTRIBUIDORA', '-')}</td>"
            f"<td>{row.get('REF_ATUAL', '-')}</td>"
            f"<td>{row.get('REF_ANTERIOR', '-')}</td>"
            f"<td>{_brl(row.get('VALOR_ATUAL', 0))}</td>"
            f"<td>{_brl(row.get('VALOR_ANT', 0))}</td>"
            f"<td>{row.get('PERC_VALOR', '-')}%</td>"
            f"</tr>"
        )
    return (
        f"{linha_gestores_html(grupo)}"
        f"<b>Alerta de Valor Total - Aumento &gt;{PERC_ALERTA}% (vs. A-1)</b><br>"
        f"{len(df)} unidade(s) com variacao relevante<br><br>"
        f"<table><tr>"
        f"<th>Unidade</th><th>Instalacao</th><th>Distribuidora</th>"
        f"<th>Ref. Atual</th><th>Ref. A-1</th>"
        f"<th>Valor Atual</th><th>Valor A-1</th><th>% Variacao</th>"
        f"</tr>{linhas}</table>"
    )


def montar_mensagem_html_vencimentos_agua(df, grupo):
    amanha = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
    if df.empty:
        return (
            f"{linha_gestores_html_agua(grupo)}"
            f"<b>Vencimentos de Agua para amanha - {amanha}</b><br>"
            "Nenhuma fatura encontrada."
        )
    linhas = ""
    for _, row in df.iterrows():
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('MATRICULA', '-')}</td>"
            f"<td>{row.get('DISTRIBUIDORA', '-')}</td>"
            f"<td>{amanha}</td>"
            f"<td>{_brl(row.get('TOTAL', 0))}</td>"
            f"</tr>"
        )
    return (
        f"{linha_gestores_html_agua(grupo)}"
        f"<b>Vencimentos de Agua para amanha - {amanha}</b><br>"
        f"{len(df)} fatura(s) &nbsp;|&nbsp; Total: {_brl(df['TOTAL'].sum())}<br><br>"
        f"<table>"
        f"<tr><th>Unidade</th><th>Matricula</th><th>Distribuidora</th><th>Vencimento</th><th>Valor (R$)</th></tr>"
        f"{linhas}</table>"
    )


def montar_mensagem_html_consumo_agua(df, grupo):
    if df.empty:
        return None
    linhas = ""
    for _, row in df.iterrows():
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('MATRICULA', '-')}</td>"
            f"<td>{row.get('DISTRIBUIDORA', '-')}</td>"
            f"<td>{row.get('REF_ATUAL', '-')}</td>"
            f"<td>{row.get('REF_ANTERIOR', '-')}</td>"
            f"<td>{row.get('CONSUMO_ATUAL', '-')} m³</td>"
            f"<td>{row.get('CONSUMO_ANT', '-')} m³</td>"
            f"<td>{row.get('PERC_CONSUMO', '-')}%</td>"
            f"</tr>"
        )
    return (
        f"{linha_gestores_html_agua(grupo)}"
        f"<b>Alerta de Consumo (Agua) - Aumento &gt;{PERC_ALERTA_CONSUMO}% (vs. A-1)</b><br>"
        f"{len(df)} unidade(s) com variacao relevante<br><br>"
        f"<table><tr>"
        f"<th>Unidade</th><th>Matricula</th><th>Distribuidora</th>"
        f"<th>Ref. Atual</th><th>Ref. A-1</th>"
        f"<th>Consumo Atual (m³)</th><th>Consumo A-1 (m³)</th><th>% Variacao</th>"
        f"</tr>{linhas}</table>"
    )


def montar_mensagem_html_valor_agua(df, grupo):
    if df.empty:
        return None
    linhas = ""
    for _, row in df.iterrows():
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('MATRICULA', '-')}</td>"
            f"<td>{row.get('DISTRIBUIDORA', '-')}</td>"
            f"<td>{row.get('REF_ATUAL', '-')}</td>"
            f"<td>{row.get('REF_ANTERIOR', '-')}</td>"
            f"<td>{_brl(row.get('VALOR_ATUAL', 0))}</td>"
            f"<td>{_brl(row.get('VALOR_ANT', 0))}</td>"
            f"<td>{row.get('PERC_VALOR', '-')}%</td>"
            f"</tr>"
        )
    return (
        f"{linha_gestores_html_agua(grupo)}"
        f"<b>Alerta de Valor Total (Agua) - Aumento &gt;{PERC_ALERTA}% (vs. A-1)</b><br>"
        f"{len(df)} unidade(s) com variacao relevante<br><br>"
        f"<table><tr>"
        f"<th>Unidade</th><th>Matricula</th><th>Distribuidora</th>"
        f"<th>Ref. Atual</th><th>Ref. A-1</th>"
        f"<th>Valor Atual</th><th>Valor A-1</th><th>% Variacao</th>"
        f"</tr>{linhas}</table>"
    )


# ==========================================================
# E-MAIL — SMTP (Office 365)
# ==========================================================
_CSS_EMAIL = """
    body  { font-family: Arial, sans-serif; font-size: 13px; color: #333333; margin: 0; padding: 20px; }
    h2    { color: #2e5fa3; margin-bottom: 4px; }
    p.sub { color: #666666; margin-top: 0; margin-bottom: 16px; }
    table { border-collapse: collapse; width: 100%; margin-top: 8px; }
    th    { background-color: #2e5fa3; color: #ffffff; padding: 8px 10px;
            text-align: left; border: 1px solid #2e5fa3; white-space: nowrap; }
    td    { padding: 7px 10px; border: 1px solid #c0c0c0; }
    tr:nth-child(even) td { background-color: #f5f7fa; }
"""


def _envolver_email(titulo, subtitulo, tabela_html):
    return (
        f'<!DOCTYPE html><html><head><meta charset="utf-8">'
        f"<style>{_CSS_EMAIL}</style></head><body>"
        f"<h2>{titulo}</h2>"
        f'<p class="sub">{subtitulo}</p>'
        f"{tabela_html}"
        f"</body></html>"
    )


def montar_email_html_emissao(df, grupo):
    if df.empty:
        return None
    linhas = ""
    for _, row in df.iterrows():
        ultima = row.get("ULTIMA_EMISSAO")
        ultima_fmt = pd.Timestamp(ultima).strftime("%d/%m/%Y") if pd.notna(ultima) else "Sem emissao"
        dias_raw = row.get("DIAS_SEM_EMISSAO")
        dias = int(dias_raw) if pd.notna(dias_raw) else "-"
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('INSTALACAO_MATRICULA', '-')}</td>"
            f"<td>{row.get('DISTRIBUIDORA', '-')}</td>"
            f"<td>{ultima_fmt}</td>"
            f"<td>{dias}</td>"
            f"</tr>"
        )
    tabela = (
        f"<table><tr>"
        f"<th>Unidade</th><th>Instalacao</th><th>Distribuidora</th>"
        f"<th>Ultima Emissao</th><th>Dias sem Emissao</th>"
        f"</tr>{linhas}</table>"
    )
    return _envolver_email(
        titulo=f"Emiss&otilde;es Atrasadas (&gt;50 dias) &mdash; {grupo}",
        subtitulo=f"{len(df)} unidade(s) sem emiss&atilde;o h&aacute; mais de 50 dias",
        tabela_html=tabela,
    )


def montar_email_html_vencimentos(df, grupo):
    if df.empty:
        return None
    amanha = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
    linhas = ""
    for _, row in df.iterrows():
        linhas += (
            f"<tr>"
            f"<td>{row.get('GRUPO', '-')}</td>"
            f"<td>{row.get('COD_INSTALACAO', '-')}</td>"
            f"<td>{row.get('DISTRIBUIDORA', '-')}</td>"
            f"<td>{amanha}</td>"
            f"<td>{_brl(row.get('VALOR_TOTAL', 0))}</td>"
            f"</tr>"
        )
    tabela = (
        f"<table><tr>"
        f"<th>Grupo</th><th>Instalacao</th><th>Distribuidora</th>"
        f"<th>Vencimento</th><th>Valor (R$)</th>"
        f"</tr>{linhas}</table>"
    )
    return _envolver_email(
        titulo=f"Vencimentos para amanh&atilde; &mdash; {grupo}",
        subtitulo=f"{len(df)} fatura(s) &nbsp;|&nbsp; Total: {_brl(df['VALOR_TOTAL'].sum())}",
        tabela_html=tabela,
    )


def montar_email_html_consumo(df, grupo):
    if df.empty:
        return None
    linhas = ""
    for _, row in df.iterrows():
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('COD_INSTALACAO', '-')}</td>"
            f"<td>{row.get('DISTRIBUIDORA', '-')}</td>"
            f"<td>{row.get('REF_ATUAL', '-')}</td>"
            f"<td>{row.get('REF_ANTERIOR', '-')}</td>"
            f"<td>{row.get('CONSUMO_ATUAL', '-')}</td>"
            f"<td>{row.get('CONSUMO_ANT', '-')}</td>"
            f"<td>{row.get('PERC_CONSUMO', '-')}%</td>"
            f"</tr>"
        )
    tabela = (
        f"<table><tr>"
        f"<th>Unidade</th><th>Instalacao</th><th>Distribuidora</th>"
        f"<th>Ref. Atual</th><th>Ref. A-1</th>"
        f"<th>Consumo Atual (kWh)</th><th>Consumo A-1 (kWh)</th><th>% Variacao</th>"
        f"</tr>{linhas}</table>"
    )
    return _envolver_email(
        titulo=f"Alerta de Consumo &mdash; {grupo}",
        subtitulo=f"Aumento &gt;{PERC_ALERTA_CONSUMO}% vs. mesmo m&ecirc;s do ano anterior &nbsp;|&nbsp; {len(df)} unidade(s)",
        tabela_html=tabela,
    )


def montar_email_html_valor(df, grupo):
    if df.empty:
        return None
    linhas = ""
    for _, row in df.iterrows():
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('COD_INSTALACAO', '-')}</td>"
            f"<td>{row.get('DISTRIBUIDORA', '-')}</td>"
            f"<td>{row.get('REF_ATUAL', '-')}</td>"
            f"<td>{row.get('REF_ANTERIOR', '-')}</td>"
            f"<td>{_brl(row.get('VALOR_ATUAL', 0))}</td>"
            f"<td>{_brl(row.get('VALOR_ANT', 0))}</td>"
            f"<td>{row.get('PERC_VALOR', '-')}%</td>"
            f"</tr>"
        )
    tabela = (
        f"<table><tr>"
        f"<th>Unidade</th><th>Instalacao</th><th>Distribuidora</th>"
        f"<th>Ref. Atual</th><th>Ref. A-1</th>"
        f"<th>Valor Atual</th><th>Valor A-1</th><th>% Variacao</th>"
        f"</tr>{linhas}</table>"
    )
    return _envolver_email(
        titulo=f"Alerta de Valor Total &mdash; {grupo}",
        subtitulo=f"Aumento &gt;{PERC_ALERTA}% vs. mesmo m&ecirc;s do ano anterior &nbsp;|&nbsp; {len(df)} unidade(s)",
        tabela_html=tabela,
    )


def montar_email_html_vencimentos_agua(df, grupo):
    if df.empty:
        return None
    amanha = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
    linhas = ""
    for _, row in df.iterrows():
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('MATRICULA', '-')}</td>"
            f"<td>{row.get('DISTRIBUIDORA', '-')}</td>"
            f"<td>{amanha}</td>"
            f"<td>{_brl(row.get('TOTAL', 0))}</td>"
            f"</tr>"
        )
    tabela = (
        f"<table><tr>"
        f"<th>Unidade</th><th>Matricula</th><th>Distribuidora</th>"
        f"<th>Vencimento</th><th>Valor (R$)</th>"
        f"</tr>{linhas}</table>"
    )
    return _envolver_email(
        titulo=f"Vencimentos de &Aacute;gua para amanh&atilde; &mdash; {grupo}",
        subtitulo=f"{len(df)} fatura(s) &nbsp;|&nbsp; Total: {_brl(df['TOTAL'].sum())}",
        tabela_html=tabela,
    )


def montar_email_html_consumo_agua(df, grupo):
    if df.empty:
        return None
    linhas = ""
    for _, row in df.iterrows():
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('MATRICULA', '-')}</td>"
            f"<td>{row.get('DISTRIBUIDORA', '-')}</td>"
            f"<td>{row.get('REF_ATUAL', '-')}</td>"
            f"<td>{row.get('REF_ANTERIOR', '-')}</td>"
            f"<td>{row.get('CONSUMO_ATUAL', '-')} m³</td>"
            f"<td>{row.get('CONSUMO_ANT', '-')} m³</td>"
            f"<td>{row.get('PERC_CONSUMO', '-')}%</td>"
            f"</tr>"
        )
    tabela = (
        f"<table><tr>"
        f"<th>Unidade</th><th>Matricula</th><th>Distribuidora</th>"
        f"<th>Ref. Atual</th><th>Ref. A-1</th>"
        f"<th>Consumo Atual (m³)</th><th>Consumo A-1 (m³)</th><th>% Variacao</th>"
        f"</tr>{linhas}</table>"
    )
    return _envolver_email(
        titulo=f"Alerta de Consumo (&Aacute;gua) &mdash; {grupo}",
        subtitulo=f"Aumento &gt;{PERC_ALERTA_CONSUMO}% vs. mesmo m&ecirc;s do ano anterior &nbsp;|&nbsp; {len(df)} unidade(s)",
        tabela_html=tabela,
    )


def montar_email_html_valor_agua(df, grupo):
    if df.empty:
        return None
    linhas = ""
    for _, row in df.iterrows():
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('MATRICULA', '-')}</td>"
            f"<td>{row.get('DISTRIBUIDORA', '-')}</td>"
            f"<td>{row.get('REF_ATUAL', '-')}</td>"
            f"<td>{row.get('REF_ANTERIOR', '-')}</td>"
            f"<td>{_brl(row.get('VALOR_ATUAL', 0))}</td>"
            f"<td>{_brl(row.get('VALOR_ANT', 0))}</td>"
            f"<td>{row.get('PERC_VALOR', '-')}%</td>"
            f"</tr>"
        )
    tabela = (
        f"<table><tr>"
        f"<th>Unidade</th><th>Matricula</th><th>Distribuidora</th>"
        f"<th>Ref. Atual</th><th>Ref. A-1</th>"
        f"<th>Valor Atual</th><th>Valor A-1</th><th>% Variacao</th>"
        f"</tr>{linhas}</table>"
    )
    return _envolver_email(
        titulo=f"Alerta de Valor Total (&Aacute;gua) &mdash; {grupo}",
        subtitulo=f"Aumento &gt;{PERC_ALERTA}% vs. mesmo m&ecirc;s do ano anterior &nbsp;|&nbsp; {len(df)} unidade(s)",
        tabela_html=tabela,
    )


def enviar_email(assunto, corpo_html, destinatarios, cc=None):
    """Envia e-mail HTML via SMTP Office 365 (porta 587, STARTTLS). cc recebe EMAILS_CC por padrão."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = assunto
    msg["From"]    = EMAIL_REMETENTE
    msg["To"]      = ", ".join(destinatarios)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg.attach(MIMEText(corpo_html, "html", "utf-8"))
    todos = destinatarios + (cc or [])
    with smtplib.SMTP(SMTP_CONFIG["host"], SMTP_CONFIG["port"]) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(SMTP_CONFIG["user"], SMTP_CONFIG["password"])
        smtp.sendmail(EMAIL_REMETENTE, todos, msg.as_string())


# ==========================================================
# HELPERS — TERMINAL
# ==========================================================
def _cabecalho(titulo, cor=C.AZUL):
    print(f"\n{cor}{C.B}{'=' * 52}{C.R}")
    print(f"{cor}{C.B}  {titulo}{C.R}")
    print(f"{cor}{C.B}{'=' * 52}{C.R}")

def _passo(n, total, texto):
    print(f"\n  {C.CINZA}[{n}/{total}]{C.R} {texto}")

def _ok():
    print(f"   {C.VERDE}✓ Enviado (Teams + e-mail){C.R}")

def _vazio(texto):
    print(f"   {C.AMARELO}! {texto}{C.R}")


# ==========================================================
# TAREFAS INDIVIDUAIS
# ==========================================================
def executar_vencimentos():
    _cabecalho("ENERGIA  ›  Vencimentos de amanha", C.AZUL)
    _passo(1, 2, "Buscando vencimentos...")
    df = buscar_vencimentos_amanha()
    print(f"      {len(df)} fatura(s) encontrada(s).")
    if df.empty:
        _vazio("Nenhum vencimento encontrado. Pulando envios.")
        return

    _passo(2, 2, "Enviando por grupo (Teams + e-mail)...")
    df = df[df["GRUPO"].isin(GESTORES_POR_GRUPO)]
    for grupo in df["GRUPO"].unique():
        df_grupo = df[df["GRUPO"] == grupo].reset_index(drop=True)
        print(f"\n   Grupo: {C.AZUL}{grupo}{C.R} ({len(df_grupo)} fatura(s))")
        enviar_grupo_com_chunks(df_grupo, grupo, montar_mensagem_html)
        corpo_email = montar_email_html_vencimentos(df_grupo, grupo)
        if corpo_email:
            amanha = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
            assunto = f"[Vencimentos] {grupo} - {len(df_grupo)} fatura(s) para {amanha}"
            enviar_email(assunto, corpo_email, emails_gestores(grupo), cc=EMAILS_CC)
        _ok()
    print(f"\n{C.VERDE}Tarefa concluida.{C.R}")


def executar_emissoes():
    _cabecalho("ENERGIA  ›  Emissoes atrasadas", C.AZUL)
    _passo(1, 2, "Buscando unidades com emissao atrasada (>50 dias)...")
    df = buscar_unidades_sem_emissao()
    print(f"      {len(df)} unidade(s) encontrada(s).")
    if df.empty:
        _vazio("Nenhuma unidade com emissao atrasada. Pulando envios.")
        return

    _passo(2, 2, "Enviando por grupo (Teams + e-mail)...")
    df = df[df["GRUPO"].isin(GESTORES_POR_GRUPO)]
    for grupo in df["GRUPO"].unique():
        df_grupo = df[df["GRUPO"] == grupo].reset_index(drop=True)
        print(f"\n   Grupo: {C.AZUL}{grupo}{C.R} ({len(df_grupo)} unidade(s))")
        enviar_grupo_com_chunks(df_grupo, grupo, montar_mensagem_html_emissao)
        corpo_email = montar_email_html_emissao(df_grupo, grupo)
        if corpo_email:
            assunto = f"[Emissoes Atrasadas] {grupo} - {len(df_grupo)} unidade(s) sem emissao >50 dias"
            enviar_email(assunto, corpo_email, emails_gestores(grupo), cc=EMAILS_CC)
        _ok()
    print(f"\n{C.VERDE}Tarefa concluida.{C.R}")


def executar_consumo():
    _cabecalho("ENERGIA  ›  Variacao de consumo (vs. A-1)", C.AZUL)
    _passo(1, 2, f"Buscando variacao de consumo (>{PERC_ALERTA_CONSUMO}%, janela {DIAS_TOLERANCIA}d)...")
    df = buscar_variacao_consumo()
    print(f"      {len(df)} unidade(s) com variacao acima de {PERC_ALERTA_CONSUMO}%.")
    if df.empty:
        _vazio("Nenhuma variacao relevante encontrada. Pulando envios.")
        return

    _passo(2, 2, "Enviando por grupo (Teams + e-mail)...")
    df = df[df["GRUPO"].isin(GESTORES_POR_GRUPO)]
    for grupo in df["GRUPO"].unique():
        df_grupo = df[df["GRUPO"] == grupo].reset_index(drop=True)
        print(f"\n   Grupo: {C.AZUL}{grupo}{C.R} ({len(df_grupo)} unidade(s))")
        enviar_grupo_com_chunks(df_grupo, grupo, montar_mensagem_html_consumo)
        corpo_email = montar_email_html_consumo(df_grupo, grupo)
        if corpo_email:
            assunto = f"[Alerta Consumo] {grupo} - Aumento >{PERC_ALERTA_CONSUMO}% vs. A-1"
            enviar_email(assunto, corpo_email, emails_gestores(grupo), cc=EMAILS_CC)
        _ok()
    print(f"\n{C.VERDE}Tarefa concluida.{C.R}")


def executar_valores():
    _cabecalho("ENERGIA  ›  Variacao de valor total (vs. A-1)", C.AZUL)
    _passo(1, 2, f"Buscando variacao de valor total (>{PERC_ALERTA}%, janela {DIAS_TOLERANCIA}d)...")
    df = buscar_variacao_valor()
    print(f"      {len(df)} unidade(s) com variacao acima de {PERC_ALERTA}%.")
    if df.empty:
        _vazio("Nenhuma variacao relevante encontrada. Pulando envios.")
        return

    _passo(2, 2, "Enviando por grupo (Teams + e-mail)...")
    df = df[df["GRUPO"].isin(GESTORES_POR_GRUPO)]
    for grupo in df["GRUPO"].unique():
        df_grupo = df[df["GRUPO"] == grupo].reset_index(drop=True)
        print(f"\n   Grupo: {C.AZUL}{grupo}{C.R} ({len(df_grupo)} unidade(s))")
        enviar_grupo_com_chunks(df_grupo, grupo, montar_mensagem_html_valor)
        corpo_email = montar_email_html_valor(df_grupo, grupo)
        if corpo_email:
            assunto = f"[Alerta Valor] {grupo} - Aumento >{PERC_ALERTA}% vs. A-1"
            enviar_email(assunto, corpo_email, emails_gestores(grupo), cc=EMAILS_CC)
        _ok()
    print(f"\n{C.VERDE}Tarefa concluida.{C.R}")


def executar_vencimentos_agua():
    _cabecalho("AGUA  ›  Vencimentos de amanha", C.CIANO)
    _passo(1, 2, "Buscando vencimentos de agua para amanha...")
    df = buscar_vencimentos_agua()
    print(f"      {len(df)} fatura(s) encontrada(s).")
    if df.empty:
        _vazio("Nenhum vencimento de agua encontrado. Pulando envios.")
        return

    _passo(2, 2, "Enviando por grupo (Teams + e-mail)...")
    df = df[df["GRUPO"].isin(GESTORES_POR_GRUPO_AGUA)]
    for grupo in df["GRUPO"].unique():
        df_grupo = df[df["GRUPO"] == grupo].reset_index(drop=True)
        print(f"\n   Grupo: {C.CIANO}{grupo}{C.R} ({len(df_grupo)} fatura(s))")
        mensagem = montar_mensagem_html_vencimentos_agua(df_grupo, grupo)
        if mensagem:
            enviar_via_webhook_agua(mensagem, grupo)
        corpo_email = montar_email_html_vencimentos_agua(df_grupo, grupo)
        if corpo_email:
            amanha = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
            assunto = f"[Vencimentos Agua] {grupo} - {len(df_grupo)} fatura(s) para {amanha}"
            enviar_email(assunto, corpo_email, emails_gestores_agua(grupo), cc=EMAILS_CC)
        _ok()
    print(f"\n{C.VERDE}Tarefa concluida.{C.R}")


def executar_consumo_agua():
    _cabecalho("AGUA  ›  Variacao de consumo (vs. A-1)", C.CIANO)
    _passo(1, 2, f"Buscando variacao de consumo (>{PERC_ALERTA_CONSUMO}%, janela {DIAS_TOLERANCIA}d)...")
    df = buscar_variacao_consumo_agua()
    print(f"      {len(df)} unidade(s) com variacao acima de {PERC_ALERTA_CONSUMO}%.")
    if df.empty:
        _vazio("Nenhuma variacao relevante encontrada. Pulando envios.")
        return

    _passo(2, 2, "Enviando por grupo (Teams + e-mail)...")
    df = df[df["GRUPO"].isin(GESTORES_POR_GRUPO_AGUA)]
    for grupo in df["GRUPO"].unique():
        df_grupo = df[df["GRUPO"] == grupo].reset_index(drop=True)
        print(f"\n   Grupo: {C.CIANO}{grupo}{C.R} ({len(df_grupo)} unidade(s))")
        mensagem = montar_mensagem_html_consumo_agua(df_grupo, grupo)
        if mensagem:
            enviar_via_webhook_agua(mensagem, grupo)
        corpo_email = montar_email_html_consumo_agua(df_grupo, grupo)
        if corpo_email:
            assunto = f"[Alerta Consumo Agua] {grupo} - Aumento >{PERC_ALERTA_CONSUMO}% vs. A-1"
            enviar_email(assunto, corpo_email, emails_gestores_agua(grupo), cc=EMAILS_CC)
        _ok()
    print(f"\n{C.VERDE}Tarefa concluida.{C.R}")


def executar_valores_agua():
    _cabecalho("AGUA  ›  Variacao de valor total (vs. A-1)", C.CIANO)
    _passo(1, 2, f"Buscando variacao de valor total (>{PERC_ALERTA}%, janela {DIAS_TOLERANCIA}d)...")
    df = buscar_variacao_valor_agua()
    print(f"      {len(df)} unidade(s) com variacao acima de {PERC_ALERTA}%.")
    if df.empty:
        _vazio("Nenhuma variacao relevante encontrada. Pulando envios.")
        return

    _passo(2, 2, "Enviando por grupo (Teams + e-mail)...")
    df = df[df["GRUPO"].isin(GESTORES_POR_GRUPO_AGUA)]
    for grupo in df["GRUPO"].unique():
        df_grupo = df[df["GRUPO"] == grupo].reset_index(drop=True)
        print(f"\n   Grupo: {C.CIANO}{grupo}{C.R} ({len(df_grupo)} unidade(s))")
        mensagem = montar_mensagem_html_valor_agua(df_grupo, grupo)
        if mensagem:
            enviar_via_webhook_agua(mensagem, grupo)
        corpo_email = montar_email_html_valor_agua(df_grupo, grupo)
        if corpo_email:
            assunto = f"[Alerta Valor Agua] {grupo} - Aumento >{PERC_ALERTA}% vs. A-1"
            enviar_email(assunto, corpo_email, emails_gestores_agua(grupo), cc=EMAILS_CC)
        _ok()
    print(f"\n{C.VERDE}Tarefa concluida.{C.R}")


def executar_fluxo():
    """Executa todas as tarefas em sequência."""
    print(f"\n{C.B}{'=' * 52}{C.R}")
    print(f"{C.B}  Iniciando fluxo completo{C.R}")
    print(f"{C.B}{'=' * 52}{C.R}")
    executar_vencimentos_agua()
    executar_consumo_agua()
    executar_valores_agua()
    executar_valores()
    executar_consumo()
    executar_emissoes()
    executar_vencimentos()
    print(f"\n{C.B}{'=' * 52}{C.R}")
    print(f"{C.VERDE}{C.B}  Fluxo completo concluido.{C.R}")
    print(f"{C.B}{'=' * 52}{C.R}")


# ==========================================================
# PONTO DE ENTRADA
# ==========================================================
# Agendador de Tarefas do Windows — sugestão de horários:
#   09:00  python fluxoteams.py --tarefa valores
#   10:00  python fluxoteams.py --tarefa consumos
#   11:00  python fluxoteams.py --tarefa emissoes
#   12:00  python fluxoteams.py --tarefa vencimentos
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="FluxoEnviosTeams — Alertas de Gestao de Faturas"
    )
    parser.add_argument(
        "--tarefa",
        choices=["valores", "consumos", "emissoes", "vencimentos",
                 "vencimentos_agua", "consumos_agua", "valores_agua"],
        default=None,
        help="Tarefa a executar isoladamente. Omitir executa todas em sequencia.",
    )
    args = parser.parse_args()

    TAREFAS = {
        "valores":          executar_valores,
        "consumos":         executar_consumo,
        "emissoes":         executar_emissoes,
        "vencimentos":      executar_vencimentos,
        "vencimentos_agua": executar_vencimentos_agua,
        "consumos_agua":    executar_consumo_agua,
        "valores_agua":     executar_valores_agua,
    }

    fn = TAREFAS.get(args.tarefa)
    if fn:
        fn()
    else:
        executar_fluxo()
