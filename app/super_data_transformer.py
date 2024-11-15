import pyspark.sql.functions as F


class SuperDataTransformer:
    def do_the_agg(self, df):
        df_agg = df.groupBy("name").agg(F.sum(F.col("value")).alias("sumval"))
        return df_agg

    def do_the_other_agg(self, df):
        df_agg = df.groupBy("name").agg(F.max(F.col("value")).alias("maxval"))
        return df_agg
