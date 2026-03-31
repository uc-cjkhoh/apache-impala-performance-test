import pandas as pd

from tqdm import tqdm
from time import perf_counter
from datetime import datetime
from impala.dbapi import connect


class TestPerformance:
    def __init__(self, host: str, port: int, iteration: int, queries: list[str], mt_dops: list[int]):
        self.host = host
        self.port = port
        self.iteration = iteration
        self.queries = queries
        self.mt_dops = mt_dops
        
    
    def get_performance(self): 
        try:
            conn = connect(host=self.host, port=self.port)
            cursor = conn.cursor()
            all_query_performance = {}
            
            # get impala version
            cursor.execute('SELECT VERSION()')
            impala_version = cursor.fetchall()[0][0].split('-')[0].replace(' ', '_')
            
            for (mt_dop, query) in tqdm(zip(self.mt_dops, self.queries)):
                query_performance = []
                
                cursor.execute(f"SET MT_DOP={mt_dop}")
                
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
            raise
         
    
    def save_result(self, impala_version: str, result: pd.DataFrame):
        return result.to_csv(f'apache_{impala_version}_performance_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        