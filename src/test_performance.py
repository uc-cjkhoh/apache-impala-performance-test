import pandas as pd

from tqdm import tqdm
from time import perf_counter
from datetime import datetime
from impala.dbapi import connect


class TestPerformance:
    def __init__(self, host: str, port: int, iteration: int, queries: list[str]):
        self.host = host
        self.port = port
        self.iteration = iteration
        self.queries = queries
        
    
    def get_performance(self): 
        try:
            conn = connect(host=self.host, port=self.port)
            cursor = conn.cursor()
            all_query_performance = {}
            
            # get impala version
            cursor.execute('SELECT VERSION()')
            impala_version = cursor.fetchall()[0][0].split('-')[0].replace(' ', '_')
            
            for query in tqdm(self.queries):
                query_performance = []
                
                # Single execution warm-up
                cursor.execute(query)
                
                for i in tqdm(range(self.iteration)):
                    # clear metadata cache
                    cursor.execute('INVALIDATE METADATA')
                    
                    # get starting time
                    start_time = perf_counter()
                    
                    # run query
                    cursor.execute(query)
            
                    # get ending time
                    end_time = perf_counter()
                    
                    # calculate run time
                    run_time = end_time - start_time
                    
                    # save run time
                    query_performance.append(run_time)
                    
                all_query_performance[query.strip()] = query_performance
            
            return impala_version, pd.DataFrame(all_query_performance)
            
        except Exception as e:
            self.save_result(impala_version, all_query_performance)
            raise
         
    
    def save_result(self, impala_version: str, result: pd.DataFrame):
        return result.to_csv(f'apache_{impala_version}_performance_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        