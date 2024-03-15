from config import ORIGINAL_DATA_DIR, GENERATED_DATA_DIR, SHEET_NAME, SKIP_ROWS


from .extract import Extract
from .transform import Transformer
from .load import Load

FINAl_FNAME = 'dados_finais.xlsx'


load_sheet = Extract(folder=ORIGINAL_DATA_DIR, sheet_name=SHEET_NAME, skiprows=SKIP_ROWS)
transform = Transformer()
load = Load(FINAl_FNAME)



def etl():

    df = load_sheet()
    df_transformed = transform(df)
    load(df_transformed)