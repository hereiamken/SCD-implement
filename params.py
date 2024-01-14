EOW_DATE = "9999-12-31"
SOURCE_PATH = "/Users/trankiendang/Projects/SCD-implement/data/Mall_Customers.csv"
SOURCE_CHANGED_PATH = "/Users/trankiendang/Projects/SCD-implement/data/Mall_Customers_changed.csv"
DEST_PATH = "/Users/trankiendang/Projects/SCD-implement/dest/"
TEMP_PATH = "/Users/trankiendang/Projects/SCD-implement/temp/"
KEY_LIST = ["CustomerID"]
type2_cols = ["CustomerID", "Genre", "Age",
              "Annual_Income_(k$)", "Spending_Score"]
scd2_cols = ["effective_date", "expiration_date", "current_flag"]
DATE_FORMAT = "yyyy-MM-dd"
