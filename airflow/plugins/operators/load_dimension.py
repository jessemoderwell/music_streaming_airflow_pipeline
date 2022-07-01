from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    loaddim_sql_template = """
    INSERT INTO {}
    {}
    """
        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 create_sql="",
                 select_for_insert="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.destination_table= destination_table
        self.create_sql= create_sql
        self.select_for_insert= select_for_insert

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        #Load data into destination table
        loaddim_finished_sql = LoadDimensionOperator.loaddim_sql_template.format(
            self.destination_table,
            self.select_for_insert
        )
        
        redshift_hook.run(loaddim_finished_sql)
        self.log.info(
            "{} has been loaded".format(self.destination_table)
        )
