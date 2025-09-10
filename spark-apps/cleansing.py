
from pyspark.sql.functions import col, trim

def apply_cleansing(df):
    """
    Apply cleansing rules to input DataFrame.
    """
    return df.filter(
        (col("title").isNotNull()) & (trim(col("title")) != "") &
        (col("release_year").between(1900, 2025)) &
        (trim(col("country")) != "") &
        (trim(col("genres")) != "") &
        (trim(col("actors")) != "") &
        (trim(col("directors")) != "") &
        (trim(col("composers")) != "") &
        (trim(col("screenwriters")) != "") &
        (trim(col("cinematographer")) != "") &
        (trim(col("production_companies")) != "")
    )
