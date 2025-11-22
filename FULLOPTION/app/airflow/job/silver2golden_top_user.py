from core import etl
from core.transform_base import TransformBase

class Silver2GoldenTopUser(TransformBase):
    def transform(self):
        df_lol = self.dfs.get("customer_scd4_current")
        return {
            "df_lol": df_lol
        }

etl.run("/opt/bitnami/spark/app/airflow/config/bronze2silver_customer.json")
