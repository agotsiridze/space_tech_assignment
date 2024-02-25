import etl


FILE_PATH = "data/data.csv"
OUTPUT_PATH = "data/contract_features.csv"

data_df = etl.extract_data(FILE_PATH)
data_transformer = etl.DataTransformer(data_df)
data_transformer.insert_columns()
data_transformer.transform_data("contracts")
etl.load_data(data_transformer.df, OUTPUT_PATH)
