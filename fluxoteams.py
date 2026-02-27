import os
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

# ==========================================================
# CONFIGURAÇÕES GERAIS
# ==========================================================
DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

# Webhook do Power Automate para envio no Teams
URL_WEBHOOK = os.getenv("URL_WEBHOOK")

# Configurações SMTP (Office 365)
SMTP_CONFIG = {
    "host":     os.getenv("SMTP_HOST", "smtp.office365.com"),
    "port":     int(os.getenv("SMTP_PORT", 587)),
    "user":     os.getenv("SMTP_USER"),
    "password": os.getenv("SMTP_PASS"),
}

# E-mail remetente e destinatário de teste
EMAIL_REMETENTE = os.getenv("SMTP_USER")
EMAIL_TESTE     = "guilherme.garcia@voraenergia.com.br"
PERC_ALERTA     = 30   # % de aumento para disparar alerta
CHUNK_SIZE      = 50   # linhas por lote no fallback de EntityTooLarge (413)

# Grupos a serem excluídos dos alertas
GRUPOS_EXCLUIDOS = (
    "GPA", "OI", "ENEL X GD", "VENANCIO", "CVLB",
    "BRADESCO", "TELEFONICA", "GBZEnergia", "GDS", "LIVRE ACL", "DROGAL", "REDE AMERICAS"
)

# Mapeamento de grupos para gestores.
# Cada gestor tem "email" (usado para envio e lookup no PA) e "nome" (usado na menção @Nome no Teams).
#
# ATENÇÃO — Para as menções realmente funcionarem no Teams, o fluxo Power Automate precisa:
#   1. Receber o campo "gestores" (emails separados por ";") e "gestores_nomes" (nomes separados por ";")
#   2. Para cada email, usar a ação "Get user profile (V2)" para obter o ID do usuário no Azure AD
#   3. Usar a ação "Post a message in a chat or channel" com o corpo HTML contendo <at>Nome</at>
#      e o array de entities com {type: "mention", text: "<at>Nome</at>", mentioned: {id, displayName}}
GESTORES_POR_GRUPO = {
    "ABIJCSUD":       [
        {"email": "guilherme.garcia@voraenergia.com.br", "nome": "Guilherme Garcia"},
        {"email": "wanderson.santos@voraenergia.com.br", "nome": "Wanderson Santos"},
    ],
    "DASA":           [
        {"email": "bruno.petrillo@voraenergia.com.br",   "nome": "Bruno Petrillo"},
        {"email": "sabrina.gomes@voraenergia.com.br",    "nome": "Sabrina Gomes"},
    ],
    "MAGAZINE LUIZA": [
        {"email": "guilherme.garcia@voraenergia.com.br", "nome": "Guilherme Garcia"},
    ],
    "MARISA":         [
        {"email": "gustavo.felix@voraenergia.com.br",    "nome": "Gustavo Felix"},
    ],
    "PERNAMBUCANAS":  [
        {"email": "caio.augusto@voraenergia.com.br",     "nome": "Caio Augusto"},
    ],
    "RENNER":         [
        {"email": "caio.augusto@voraenergia.com.br",     "nome": "Caio Augusto"},
    ],
    "PEPSICO":        [
        {"email": "samuel.santos@voraenergia.com.br",    "nome": "Samuel Santos"},
    ],
    "SANTANDER":      [
        {"email": "samuel.santos@voraenergia.com.br",    "nome": "Samuel Santos"},
    ],
    "ZARA":           [
        {"email": "gustavo.felix@voraenergia.com.br",    "nome": "Gustavo Felix"},
    ],
    "KORA":           [
        {"email": "guilherme.viana@voraenergia.com.br",  "nome": "Guilherme Viana"},
    ],
}


# ==========================================================
# HELPERS — GESTORES
# ==========================================================
def emails_gestores(grupo):
    return [g["email"] for g in GESTORES_POR_GRUPO.get(grupo, [])]


def nomes_gestores(grupo):
    return [g["nome"] for g in GESTORES_POR_GRUPO.get(grupo, [])]


def linha_gestores_html(grupo):
    """Retorna linha HTML com menções <at>Nome</at> (formato Teams).
    O Power Automate usa esses nomes junto com os IDs de usuário para montar as menções reais."""
    gestores = GESTORES_POR_GRUPO.get(grupo, [])
    if not gestores:
        return ""
    mencoes = ", ".join(f'<at>{g["nome"]}</at>' for g in gestores)
    return f"<b>Gestores:</b> {mencoes}<br><br>"


# ==========================================================
# BANCO DE DADOS
# ==========================================================
def buscar_unidades_sem_emissao():
    """
    Busca todas as unidades ativas de ENERGIA e calcula quantos dias
    se passaram desde a última DATA_EMISSAO em tb_dfat_gestao_faturas_energia_novo.
    Retorna apenas unidades com mais de 50 dias sem emissão (ou sem nenhuma emissão).
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
        HAVING DIAS_SEM_EMISSAO > 50 OR ULTIMA_EMISSAO IS NULL
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


def buscar_variacao_consumo():
    """
    Compara o consumo (FP e P) da referência mais recente (>= 2026) com o mesmo mês do ano anterior.
    Retorna unidades com variação de consumo FP ou P acima de PERC_ALERTA %.
    """
    excluidos = ", ".join(f"'{g}'" for g in GRUPOS_EXCLUIDOS)
    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
        SELECT
            a.COD_INSTALACAO,
            c.GRUPO,
            c.NOME_UNIDADE,
            DATE_FORMAT(a.REFERENCIA, '%Y%m')                              AS REF_ATUAL,
            DATE_FORMAT(DATE_SUB(a.REFERENCIA, INTERVAL 1 YEAR), '%Y%m')  AS REF_ANTERIOR,
            a.CONSUMO_LIDO_FP                                              AS FP_ATUAL,
            ant.CONSUMO_LIDO_FP                                            AS FP_ANT,
            ROUND(((a.CONSUMO_LIDO_FP  - ant.CONSUMO_LIDO_FP)  / NULLIF(ant.CONSUMO_LIDO_FP,  0)) * 100, 1) AS PERC_FP,
            a.CONSUMO_LIDO_P                                               AS P_ATUAL,
            ant.CONSUMO_LIDO_P                                             AS P_ANT,
            ROUND(((a.CONSUMO_LIDO_P   - ant.CONSUMO_LIDO_P)   / NULLIF(ant.CONSUMO_LIDO_P,   0)) * 100, 1) AS PERC_P
        FROM tb_dfat_gestao_faturas_energia_novo AS a
        INNER JOIN (
            SELECT COD_INSTALACAO, MAX(REFERENCIA) AS MAX_REF
            FROM tb_dfat_gestao_faturas_energia_novo
            WHERE YEAR(REFERENCIA) >= 2026
            GROUP BY COD_INSTALACAO
        ) AS ult ON a.COD_INSTALACAO = ult.COD_INSTALACAO AND a.REFERENCIA = ult.MAX_REF
        INNER JOIN tb_dfat_gestao_faturas_energia_novo AS ant
            ON a.COD_INSTALACAO = ant.COD_INSTALACAO
           AND ant.REFERENCIA = DATE_SUB(a.REFERENCIA, INTERVAL 1 YEAR)
        INNER JOIN tb_clientes_gestao_faturas AS c
            ON a.COD_INSTALACAO = c.INSTALACAO_MATRICULA
        WHERE c.UTILIDADE = 'ENERGIA'
          AND c.STATUS_UNIDADE = 'Ativa'
          AND c.GRUPO IS NOT NULL
          AND c.GRUPO NOT IN ({excluidos})
          AND YEAR(a.REFERENCIA) >= 2026
        HAVING PERC_FP > {PERC_ALERTA} OR PERC_P > {PERC_ALERTA}
        ORDER BY c.GRUPO, c.NOME_UNIDADE
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def buscar_variacao_valor():
    """
    Compara o VALOR_TOTAL da referência mais recente (>= 2026) com o mesmo mês do ano anterior.
    Retorna unidades com variação de valor acima de PERC_ALERTA %.
    """
    excluidos = ", ".join(f"'{g}'" for g in GRUPOS_EXCLUIDOS)
    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
        SELECT
            a.COD_INSTALACAO,
            c.GRUPO,
            c.NOME_UNIDADE,
            DATE_FORMAT(a.REFERENCIA, '%Y%m')                              AS REF_ATUAL,
            DATE_FORMAT(DATE_SUB(a.REFERENCIA, INTERVAL 1 YEAR), '%Y%m')  AS REF_ANTERIOR,
            a.VALOR_TOTAL                                                  AS VALOR_ATUAL,
            ant.VALOR_TOTAL                                                AS VALOR_ANT,
            ROUND(((a.VALOR_TOTAL - ant.VALOR_TOTAL) / NULLIF(ant.VALOR_TOTAL, 0)) * 100, 1) AS PERC_VALOR
        FROM tb_dfat_gestao_faturas_energia_novo AS a
        INNER JOIN (
            SELECT COD_INSTALACAO, MAX(REFERENCIA) AS MAX_REF
            FROM tb_dfat_gestao_faturas_energia_novo
            WHERE YEAR(REFERENCIA) >= 2026
            GROUP BY COD_INSTALACAO
        ) AS ult ON a.COD_INSTALACAO = ult.COD_INSTALACAO AND a.REFERENCIA = ult.MAX_REF
        INNER JOIN tb_dfat_gestao_faturas_energia_novo AS ant
            ON a.COD_INSTALACAO = ant.COD_INSTALACAO
           AND ant.REFERENCIA = DATE_SUB(a.REFERENCIA, INTERVAL 1 YEAR)
        INNER JOIN tb_clientes_gestao_faturas AS c
            ON a.COD_INSTALACAO = c.INSTALACAO_MATRICULA
        WHERE c.UTILIDADE = 'ENERGIA'
          AND c.STATUS_UNIDADE = 'Ativa'
          AND c.GRUPO IS NOT NULL
          AND c.GRUPO NOT IN ({excluidos})
          AND YEAR(a.REFERENCIA) >= 2026
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
    """Envia mensagem HTML para o Teams via webhook (Power Automate).
    Passa 'grupo', 'message', 'gestores' (emails; sep) e 'gestores_nomes' (nomes; sep)
    para o fluxo PA rotear e construir as menções reais no Teams."""
    resp = requests.post(
        URL_WEBHOOK,
        json={
            "grupo":           grupo,
            "message":         mensagem_html,
            "gestores":        ";".join(emails_gestores(grupo)),  # emails para lookup de ID no PA
            "gestores_nomes":  ";".join(nomes_gestores(grupo)),   # nomes para texto da menção
        },
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    print(f"   Status: {resp.status_code}")
    if resp.status_code not in (200, 201, 202):
        print(f"   Resposta: {resp.text[:500]}")
    resp.raise_for_status()


def enviar_grupo_com_chunks(df, grupo, montar_fn):
    """Envia mensagem para o Teams via webhook.
    Se a resposta for 413 (EntityTooLarge), divide o DataFrame em lotes de CHUNK_SIZE
    e reenvia cada lote separadamente."""
    mensagem = montar_fn(df, grupo)
    if not mensagem:
        return
    try:
        enviar_via_webhook(mensagem, grupo)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 413:
            total_lotes = (len(df) - 1) // CHUNK_SIZE + 1
            print(f"   EntityTooLarge — enviando em {total_lotes} lote(s) de até {CHUNK_SIZE} linhas...")
            for i in range(0, len(df), CHUNK_SIZE):
                chunk = df.iloc[i:i + CHUNK_SIZE].reset_index(drop=True)
                mensagem_chunk = montar_fn(chunk, grupo)
                if mensagem_chunk:
                    enviar_via_webhook(mensagem_chunk, grupo)
                    print(f"   Lote {i // CHUNK_SIZE + 1}/{total_lotes} enviado.")
        else:
            raise


def montar_mensagem_html_emissao(df, grupo):
    """Monta tabela HTML com unidades com emissão atrasada (>50 dias)."""
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

    return (
        f"{linha_gestores_html(grupo)}"
        f"<b>Unidades sem emissao (&gt;50 dias)</b><br>"
        f"{len(df)} unidade(s) com atraso<br><br>"
        f"<table>"
        f"<tr><th>Unidade</th><th>Instalacao</th><th>Ultima Emissao</th><th>Dias</th></tr>"
        f"{linhas}"
        f"</table>"
    )


def montar_mensagem_html(df, grupo):
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

    return (
        f"{linha_gestores_html(grupo)}"
        f"<b>Vencimentos para amanha - {amanha}</b><br>"
        f"{total_faturas} fatura(s) &nbsp;|&nbsp; Total: {total_fmt}<br><br>"
        f"<table>"
        f"<tr><th>Grupo</th><th>Instalacao</th><th>Vencimento</th><th>Valor (R$)</th></tr>"
        f"{linhas}"
        f"</table>"
    )


def montar_mensagem_html_consumo(df, grupo):
    """Monta tabela HTML com unidades com variação de consumo (FP ou P) acima de PERC_ALERTA %."""
    if df.empty:
        return None

    linhas = ""
    for _, row in df.iterrows():
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('COD_INSTALACAO', '-')}</td>"
            f"<td>{row.get('REF_ATUAL', '-')}</td>"
            f"<td>{row.get('REF_ANTERIOR', '-')}</td>"
            f"<td>{row.get('FP_ATUAL', '-')}</td>"
            f"<td>{row.get('FP_ANT', '-')}</td>"
            f"<td>{row.get('PERC_FP', '-')}%</td>"
            f"<td>{row.get('P_ATUAL', '-')}</td>"
            f"<td>{row.get('P_ANT', '-')}</td>"
            f"<td>{row.get('PERC_P', '-')}%</td>"
            f"</tr>"
        )

    return (
        f"{linha_gestores_html(grupo)}"
        f"<b>Alerta de Consumo - Aumento &gt;{PERC_ALERTA}% (vs. A-1)</b><br>"
        f"{len(df)} unidade(s) com variacao relevante<br><br>"
        f"<table>"
        f"<tr>"
        f"<th>Unidade</th><th>Instalacao</th>"
        f"<th>Ref. Atual</th><th>Ref. A-1</th>"
        f"<th>FP Atual</th><th>FP A-1</th><th>% FP</th>"
        f"<th>P Atual</th><th>P A-1</th><th>% P</th>"
        f"</tr>"
        f"{linhas}"
        f"</table>"
    )


def montar_mensagem_html_valor(df, grupo):
    """Monta tabela HTML com unidades com variação de valor total acima de PERC_ALERTA %."""
    if df.empty:
        return None

    linhas = ""
    for _, row in df.iterrows():
        v_atual = float(row.get("VALOR_ATUAL", 0) or 0)
        v_ant   = float(row.get("VALOR_ANT",   0) or 0)
        v_atual_fmt = f"R$ {v_atual:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        v_ant_fmt   = f"R$ {v_ant:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('COD_INSTALACAO', '-')}</td>"
            f"<td>{row.get('REF_ATUAL', '-')}</td>"
            f"<td>{row.get('REF_ANTERIOR', '-')}</td>"
            f"<td>{v_atual_fmt}</td>"
            f"<td>{v_ant_fmt}</td>"
            f"<td>{row.get('PERC_VALOR', '-')}%</td>"
            f"</tr>"
        )

    return (
        f"{linha_gestores_html(grupo)}"
        f"<b>Alerta de Valor Total - Aumento &gt;{PERC_ALERTA}% (vs. A-1)</b><br>"
        f"{len(df)} unidade(s) com variacao relevante<br><br>"
        f"<table>"
        f"<tr>"
        f"<th>Unidade</th><th>Instalacao</th>"
        f"<th>Ref. Atual</th><th>Ref. A-1</th>"
        f"<th>Valor Atual</th><th>Valor A-1</th><th>% Variacao</th>"
        f"</tr>"
        f"{linhas}"
        f"</table>"
    )


# ==========================================================
# E-MAIL — SMTP (Office 365)
# ==========================================================

# CSS inline para os e-mails com tabela formatada
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
    """Envolve o conteúdo em um documento HTML completo com estilos."""
    return (
        f'<!DOCTYPE html><html><head><meta charset="utf-8">'
        f"<style>{_CSS_EMAIL}</style></head><body>"
        f"<h2>{titulo}</h2>"
        f'<p class="sub">{subtitulo}</p>'
        f"{tabela_html}"
        f"</body></html>"
    )


def montar_email_html_consumo(df, grupo):
    """Monta e-mail HTML formatado com tabela bordada para alerta de consumo."""
    if df.empty:
        return None

    linhas = ""
    for _, row in df.iterrows():
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('COD_INSTALACAO', '-')}</td>"
            f"<td>{row.get('REF_ATUAL', '-')}</td>"
            f"<td>{row.get('REF_ANTERIOR', '-')}</td>"
            f"<td>{row.get('FP_ATUAL', '-')}</td>"
            f"<td>{row.get('FP_ANT', '-')}</td>"
            f"<td>{row.get('PERC_FP', '-')}%</td>"
            f"<td>{row.get('P_ATUAL', '-')}</td>"
            f"<td>{row.get('P_ANT', '-')}</td>"
            f"<td>{row.get('PERC_P', '-')}%</td>"
            f"</tr>"
        )

    tabela = (
        f"<table>"
        f"<tr>"
        f"<th>Unidade</th><th>Instalacao</th>"
        f"<th>Ref. Atual</th><th>Ref. A-1</th>"
        f"<th>FP Atual</th><th>FP A-1</th><th>% FP</th>"
        f"<th>P Atual</th><th>P A-1</th><th>% P</th>"
        f"</tr>{linhas}</table>"
    )

    return _envolver_email(
        titulo=f"Alerta de Consumo &mdash; {grupo}",
        subtitulo=f"Aumento &gt;{PERC_ALERTA}% vs. mesmo m&ecirc;s do ano anterior &nbsp;|&nbsp; {len(df)} unidade(s)",
        tabela_html=tabela,
    )


def montar_email_html_valor(df, grupo):
    """Monta e-mail HTML formatado com tabela bordada para alerta de valor total."""
    if df.empty:
        return None

    linhas = ""
    for _, row in df.iterrows():
        v_atual = float(row.get("VALOR_ATUAL", 0) or 0)
        v_ant   = float(row.get("VALOR_ANT",   0) or 0)
        v_atual_fmt = f"R$ {v_atual:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        v_ant_fmt   = f"R$ {v_ant:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        linhas += (
            f"<tr>"
            f"<td>{row.get('NOME_UNIDADE', '-')}</td>"
            f"<td>{row.get('COD_INSTALACAO', '-')}</td>"
            f"<td>{row.get('REF_ATUAL', '-')}</td>"
            f"<td>{row.get('REF_ANTERIOR', '-')}</td>"
            f"<td>{v_atual_fmt}</td>"
            f"<td>{v_ant_fmt}</td>"
            f"<td>{row.get('PERC_VALOR', '-')}%</td>"
            f"</tr>"
        )

    tabela = (
        f"<table>"
        f"<tr>"
        f"<th>Unidade</th><th>Instalacao</th>"
        f"<th>Ref. Atual</th><th>Ref. A-1</th>"
        f"<th>Valor Atual</th><th>Valor A-1</th><th>% Variacao</th>"
        f"</tr>{linhas}</table>"
    )

    return _envolver_email(
        titulo=f"Alerta de Valor Total &mdash; {grupo}",
        subtitulo=f"Aumento &gt;{PERC_ALERTA}% vs. mesmo m&ecirc;s do ano anterior &nbsp;|&nbsp; {len(df)} unidade(s)",
        tabela_html=tabela,
    )


def enviar_email(assunto, corpo_html, destinatarios):
    """Envia e-mail HTML via SMTP Office 365 (smtp.office365.com:587 com STARTTLS)."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = assunto
    msg["From"]    = EMAIL_REMETENTE
    msg["To"]      = ", ".join(destinatarios)
    msg.attach(MIMEText(corpo_html, "html", "utf-8"))

    with smtplib.SMTP(SMTP_CONFIG["host"], SMTP_CONFIG["port"]) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(SMTP_CONFIG["user"], SMTP_CONFIG["password"])
        smtp.sendmail(EMAIL_REMETENTE, destinatarios, msg.as_string())


# ==========================================================
# TAREFAS INDIVIDUAIS
# (cada uma pode ser chamada isoladamente via --tarefa)
# ==========================================================
def executar_vencimentos():
    print("\n" + "=" * 50)
    print("TAREFA: Vencimentos de amanha")
    print("=" * 50)

    print("\n[1/2] Buscando vencimentos de amanha...")
    df = buscar_vencimentos_amanha()
    print(f"      {len(df)} fatura(s) encontrada(s).")

    print("\n[2/2] Enviando por grupo...")
    for grupo in df["GRUPO"].unique():
        df_grupo = df[df["GRUPO"] == grupo].reset_index(drop=True)
        print(f"\n   Grupo: {grupo} ({len(df_grupo)} fatura(s))")
        enviar_grupo_com_chunks(df_grupo, grupo, montar_mensagem_html)
        print("   Enviado com sucesso!")

    print("\nTarefa concluida.")


def executar_emissoes():
    print("\n" + "=" * 50)
    print("TAREFA: Emissoes atrasadas")
    print("=" * 50)

    print("\n[1/2] Buscando unidades com emissao atrasada (>50 dias)...")
    df = buscar_unidades_sem_emissao()
    print(f"      {len(df)} unidade(s) encontrada(s).")

    print("\n[2/2] Enviando por grupo...")
    for grupo in df["GRUPO"].unique():
        df_grupo = df[df["GRUPO"] == grupo].reset_index(drop=True)
        print(f"\n   Grupo: {grupo} ({len(df_grupo)} unidade(s))")
        enviar_grupo_com_chunks(df_grupo, grupo, montar_mensagem_html_emissao)
        print("   Enviado com sucesso!")

    print("\nTarefa concluida.")


def executar_consumo():
    print("\n" + "=" * 50)
    print("TAREFA: Variacao de consumo (vs. A-1)")
    print("=" * 50)

    print(f"\n[1/2] Buscando variacao de consumo (>= 2026, vs. A-1)...")
    df = buscar_variacao_consumo()
    print(f"      {len(df)} unidade(s) com variacao acima de {PERC_ALERTA}%.")

    print("\n[2/2] Enviando por grupo (Teams + e-mail)...")
    for grupo in df["GRUPO"].unique():
        df_grupo = df[df["GRUPO"] == grupo].reset_index(drop=True)
        print(f"\n   Grupo: {grupo} ({len(df_grupo)} unidade(s))")
        enviar_grupo_com_chunks(df_grupo, grupo, montar_mensagem_html_consumo)
        corpo_email = montar_email_html_consumo(df_grupo, grupo)
        if corpo_email:
            assunto = f"[Alerta Consumo] {grupo} - Aumento >{PERC_ALERTA}% vs. A-1"
            enviar_email(assunto, corpo_email, [EMAIL_TESTE])
        print("   Enviado com sucesso (Teams + e-mail)!")

    print("\nTarefa concluida.")


def executar_valores():
    print("\n" + "=" * 50)
    print("TAREFA: Variacao de valor total (vs. A-1)")
    print("=" * 50)

    print(f"\n[1/2] Buscando variacao de valor total (>= 2026, vs. A-1)...")
    df = buscar_variacao_valor()
    print(f"      {len(df)} unidade(s) com variacao acima de {PERC_ALERTA}%.")

    print("\n[2/2] Enviando por grupo (Teams + e-mail)...")
    for grupo in df["GRUPO"].unique():
        df_grupo = df[df["GRUPO"] == grupo].reset_index(drop=True)
        print(f"\n   Grupo: {grupo} ({len(df_grupo)} unidade(s))")
        enviar_grupo_com_chunks(df_grupo, grupo, montar_mensagem_html_valor)
        corpo_email = montar_email_html_valor(df_grupo, grupo)
        if corpo_email:
            assunto = f"[Alerta Valor] {grupo} - Aumento >{PERC_ALERTA}% vs. A-1"
            enviar_email(assunto, corpo_email, [EMAIL_TESTE])
        print("   Enviado com sucesso (Teams + e-mail)!")

    print("\nTarefa concluida.")


def executar_fluxo():
    """Executa todas as tarefas em sequência (equivale a rodar sem --tarefa)."""
    print("=" * 50)
    print("Iniciando fluxo completo")
    print("=" * 50)
    executar_valores()
    executar_consumo()
    executar_emissoes()
    executar_vencimentos()
    print("\n" + "=" * 50)
    print("Fluxo completo concluido.")
    print("=" * 50)


# ==========================================================
# PONTO DE ENTRADA
# ==========================================================
# Agendador de Tarefas do Windows — configurar 4 entradas:
#   09:00  python fluxoteams.py --tarefa valores
#   10:00  python fluxoteams.py --tarefa consumos
#   11:00  python fluxoteams.py --tarefa emissoes
#   12:00  python fluxoteams.py --tarefa vencimentos
#
# Para rodar tudo de uma vez (sem agendador):
#   python fluxoteams.py
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="FluxoEnviosTeams — Alertas de Gestao de Faturas de Energia"
    )
    parser.add_argument(
        "--tarefa",
        choices=["valores", "consumos", "emissoes", "vencimentos"],
        default=None,
        help=(
            "Tarefa a executar isoladamente. "
            "Omitir executa todas as tarefas em sequencia. "
            "Valores: 09h | Consumos: 10h | Emissoes: 11h | Vencimentos: 12h"
        ),
    )
    args = parser.parse_args()

    TAREFAS = {
        "valores":     executar_valores,
        "consumos":    executar_consumo,
        "emissoes":    executar_emissoes,
        "vencimentos": executar_vencimentos,
    }

    fn = TAREFAS.get(args.tarefa)
    if fn:
        fn()
    else:
        executar_fluxo()
