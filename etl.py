import pandas as pd
import os
import glob

# Função para extrair e consolida todos os json em um

def extrair_dados(path: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(path, '*.json'))
    #print(arquivos_json)
    df_list = [pd.read_json(file) for file in arquivos_json]
    #print(df_list)
    df_final = pd.concat(df_list, ignore_index=True)
    #print(df_final)
    return df_final


# Função que transforma

def calculo_total_vendas(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe["Total"] = dataframe["Quantidade"] * dataframe["Venda"]
    return dataframe

# Uma função que dá load em csv ou parquet

def carregar_dados(df: pd.DataFrame, formato_saida: list) -> None:
    if 'csv' in formato_saida:
        df.to_csv("dados.csv", index = False)
    if 'parquet' in formato_saida:
        df.to_parquet("dados.parquet")

def pipeline_calculo_kpi_de_vendas(folder: str, formato_saida: list):
    df: pd.DataFrame = extrair_dados(folder)
    df_transformado: pd.DataFrame = calculo_total_vendas(df)
    carregar_dados(df_transformado, formato_saida)


if __name__ == '__main__':
    folder: str = 'data'
    df: pd.DataFrame = extrair_dados(folder)
    df_transformado: pd.DataFrame = calculo_total_vendas(df)
    formato_saida = ['csv', 'parquet']
    carregar_dados(df_transformado, formato_saida)
