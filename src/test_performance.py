import pandas as pd

from tqdm import tqdm
from time import perf_counter
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
                    
                all_query_performance[query] = query_performance
            
            return impala_version, pd.DataFrame(all_query_performance)
            
        except Exception as e:
            print(f'An error occurred: {e}')