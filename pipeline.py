from etl import pipeline_calculo_kpi_de_vendas


path: str = 'data'
file_format: list = ['csv', 'parquet']

pipeline_calculo_kpi_de_vendas(path, file_format)