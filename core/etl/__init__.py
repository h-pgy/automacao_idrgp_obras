from config import ORIGINAL_DATA_DIR, GENERATED_DATA_DIR, SHEET_NAME, SKIP_ROWS


from .extract import Extract


load_sheet = Extract(folder=ORIGINAL_DATA_DIR, sheet_name=SHEET_NAME, skiprows=SKIP_ROWS)
