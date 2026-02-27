import os
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
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
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

# E-mail do remetente (deve ser o mesmo do SMTP_USER) e destinatário de teste
EMAIL_REMETENTE = os.getenv("SMTP_USER")
EMAIL_TESTE     = "guilherme.garcia@voraenergia.com.br"  # trocar para GESTORES_POR_GRUPO em produção
PERC_ALERTA     = 30  # % de aumento para disparar alerta

# Grupos a serem excluídos dos alertas (ex: grandes consumidores com gestão própria ou que não queremos monitorar)
GRUPOS_EXCLUIDOS = (
    "GPA", "OI", "ENEL X GD", "VENANCIO", "CVLB",
    "BRADESCO", "TELEFONICA", "GBZEnergia", "GDS","LIVRE ACL","DROGAL","REDE AMERICAS"
)

# Mapeamento de grupos para e-mails dos gestores responsáveis (para menção no Teams)
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
    Compara o consumo (FP e P) da referência mais recente com o mesmo mês do ano anterior
    (REFERENCIA - 100, ex: 202502 → 202402).
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
        HAVING PERC_FP > {PERC_ALERTA} OR PERC_P > {PERC_ALERTA}
        ORDER BY c.GRUPO, c.NOME_UNIDADE
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def buscar_variacao_valor():
    """
    Compara o VALOR_TOTAL da referência mais recente com o mesmo mês do ano anterior
    (REFERENCIA - 100).
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
        HAVING PERC_VALOR > {PERC_ALERTA}
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
# EXECUÇÃO
# ==========================================================
def executar_fluxo():
    print("=" * 50)
    print("Iniciando fluxo")
    print("=" * 50)

    print("\n[1/8] Buscando vencimentos de amanha...")
    df_vencimentos = buscar_vencimentos_amanha()
    print(f"      {len(df_vencimentos)} fatura(s) encontrada(s).")

    print("\n[2/8] Enviando vencimentos por grupo...")
    for grupo in df_vencimentos["GRUPO"].unique():
        df_grupo = df_vencimentos[df_vencimentos["GRUPO"] == grupo].reset_index(drop=True)
        print(f"\n   Grupo: {grupo} ({len(df_grupo)} fatura(s))")
        enviar_via_webhook(montar_mensagem_html(df_grupo, grupo), grupo)
        print("   Enviado com sucesso!")

    print("\n[3/8] Buscando unidades com emissao atrasada...")
    df_emissao = buscar_unidades_sem_emissao()
    print(f"      {len(df_emissao)} unidade(s) com atraso encontrada(s).")

    print("\n[4/8] Enviando emissoes atrasadas por grupo...")
    for grupo in df_emissao["GRUPO"].unique():
        df_grupo = df_emissao[df_emissao["GRUPO"] == grupo].reset_index(drop=True)
        mensagem = montar_mensagem_html_emissao(df_grupo, grupo)
        if mensagem:
            print(f"\n   Grupo: {grupo} ({len(df_grupo)} unidade(s))")
            enviar_via_webhook(mensagem, grupo)
            print("   Enviado com sucesso!")

    print("\n[5/8] Buscando variacao de consumo (vs. A-1)...")
    df_consumo = buscar_variacao_consumo()
    print(f"      {len(df_consumo)} unidade(s) com variacao acima de {PERC_ALERTA}%.")

    print("\n[6/8] Enviando variacao de consumo por grupo...")
    for grupo in df_consumo["GRUPO"].unique():
        df_grupo = df_consumo[df_consumo["GRUPO"] == grupo].reset_index(drop=True)
        mensagem = montar_mensagem_html_consumo(df_grupo, grupo)
        if mensagem:
            print(f"\n   Grupo: {grupo} ({len(df_grupo)} unidade(s))")
            enviar_via_webhook(mensagem, grupo)
            assunto = f"[Alerta Consumo] {grupo} - Aumento >{PERC_ALERTA}% vs. A-1"
            enviar_email(assunto, mensagem, [EMAIL_TESTE])
            print("   Enviado com sucesso (Teams + e-mail)!")

    print("\n[7/8] Buscando variacao de valor total (vs. A-1)...")
    df_valor = buscar_variacao_valor()
    print(f"      {len(df_valor)} unidade(s) com variacao acima de {PERC_ALERTA}%.")

    print("\n[8/8] Enviando variacao de valor por grupo...")
    for grupo in df_valor["GRUPO"].unique():
        df_grupo = df_valor[df_valor["GRUPO"] == grupo].reset_index(drop=True)
        mensagem = montar_mensagem_html_valor(df_grupo, grupo)
        if mensagem:
            print(f"\n   Grupo: {grupo} ({len(df_grupo)} unidade(s))")
            enviar_via_webhook(mensagem, grupo)
            assunto = f"[Alerta Valor] {grupo} - Aumento >{PERC_ALERTA}% vs. A-1"
            enviar_email(assunto, mensagem, [EMAIL_TESTE])
            print("   Enviado com sucesso (Teams + e-mail)!")

    print("\n" + "=" * 50)
    print("Fluxo concluido.")
    print("=" * 50)



# Permite executar o fluxo diretamente por linha de comando (python fluxoteams.py)
if __name__ == "__main__":
    executar_fluxo()
