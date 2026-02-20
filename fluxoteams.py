import os
import win32com.client as win32
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os
import pyodbc
from datetime import datetime
from openpyxl import load_workbook
from tkinter import messagebox, simpledialog
import tkinter as tk
from tkinter import ttk


GRUPOS_OPCOES = [
    "TODOS",
    "DASA",
    "ABIJCSUD",
    "RENNER",
    "KORA",
    "MAGAZINE LUIZA",
    "PERNAMBUCANAS",
    "SANTANDER",
    "MARISA",
    "GRUPO MIME",
    "ZARA",
    "PEPSICO",
    "GPA",
    "Verificar"
]

def buscar_unidades_gestao_faturas():
    conn = mysql.connector.connect(**DB_CONFIG)

    query = """
        SELECT * FROM tb_clientes_gestao_faturas
        WHERE UTILIDADE = 'ENERGIA'
        AND STATUS_UNIDADE = 'Ativa';
    """

    df_unidades = pd.read_sql(query, conn)
    conn.close()
    return df_unidades

def buscar_faturas_lidas_gestao_faturas():
    conn = mysql.connector.connect(**DB_CONFIG)

    query = """
        SELECT * FROM tb_dfat_gestao_faturas_energia_novo
    """

    df_unidades = pd.read_sql(query, conn)
    conn.close()
    return df_unidades




# ==========================================================
# CONFIGURAÇÕES GERAIS
# ==========================================================
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}


def executar_fluxo():
    print("Iniciando o fluxo de automação...") 
    return


# ==========================================================
# EXECUÇÃO
# ==========================================================

if __name__ == "__main__":
    executar_fluxo()