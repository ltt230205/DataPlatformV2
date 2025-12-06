from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from core import util



class dag_core: 
    def submit_job(args: dict) -> dict:
        job_dict={}
        for job_name, source in args.items():
                verbose = source.pop("verbose")
                if verbose == "true":
                    job_dict[job_name]=SparkSubmitOperator(**source,verbose=True)
                else:
                    job_dict[job_name]=SparkSubmitOperator(**source, verbose=False)
        return job_dict
    
    def schedule_job(args: dict, schedule_order: list):
        if len(schedule_order) <= 1:
            return

        prev_name = schedule_order[0]
        for name in schedule_order[1:]:
            args[prev_name] >> args[name]
            prev_name = name  

    def create_dag(config: dict) -> DAG:
        # Tách phần config cho DAG
        dag_keys = {
            "dag_id",
            "description",
            "schedule_interval",
            "start_date",
            "catchup",
            "default_args",
            "tags",
            "max_active_runs",
        }
        dag_kwargs = {k: v for k, v in config.items() if k in dag_keys}
        # convert datetime string
        if isinstance(dag_kwargs.get("start_date"), str):
            dag_kwargs["start_date"] = datetime.fromisoformat(dag_kwargs["start_date"])

        # convert schedule_interval="None" -> None
        if dag_kwargs.get("schedule_interval") in ("None", None):
            dag_kwargs["schedule_interval"] = None

        # convert false/true string
        if dag_kwargs.get("catchup") == 'true':
            dag_kwargs["catchup"] = True

        if dag_kwargs.get("catchup") == 'false':
            dag_kwargs["catchup"] = False


        submit_jobs_cfg = config["submit_job"]
        schedule_order = config.get("schedule", [])

        with DAG(**dag_kwargs) as dag:
            tasks = dag_core.submit_job(submit_jobs_cfg)
            dag_core.schedule_job(tasks, schedule_order)
        return dag
    
    def run(json_path: str):
        config = util.read_json(json_path)
        return dag_core.create_dag(config)

