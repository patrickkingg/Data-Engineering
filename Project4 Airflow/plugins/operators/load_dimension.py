from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
        INSERT INTO {table} 
        {select_query}
    """
    
    truncate_sql = """
        TRUNCATE TABLE {table}
    """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 truncate="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.truncate = truncate
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info("Will truncate table before inserting new data...")
            redshift.run(LoadDimensionOperator.truncate_sql.format(
                table=self.table
            ))

        self.log.info("Inserting dimension table data...")
        redshift.run(LoadDimensionOperator.insert_sql.format(
            table=self.table,
            select_query=self.select_query
        ))