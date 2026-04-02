import os
import pandas as pd


class ComparePerformance:
    def __init__(self, folder: str):
        try:
            self.files = os.listdir(folder)
            
            if len(self.files) > 0:
                valid_column = None
                valid_shape = None
                
                for file in self.files:
                    perf_history = pd.read_csv(file)
                    
                    if valid_column == None:
                        valid_column = perf_history.columns
                    
                    if valid_shape == None:
                        valid_shape = perf_history.shape
                        
                    assert (perf_history.column == valid_column).all()
                    assert (perf_history.shape == valid_shape)

        except FileNotFoundError as e:
            raise
    
    
    def get_metrics(self, perf_history: pd.DataFrame):
        '''
        Convert performance history into metrics for each column
        '''
        
        metrics = {}
        
        # -------------------------------------------
        # 1. MIN, MAX, MEDIAN, VARIANCE
        # -------------------------------------------
        # Find every columns MIN, MAX, MEDIAN, VARIANCE

        for col in perf_history.columns:
            min = perf_history[col].min
            max = perf_history[col].max
            median = perf_history[col].median
            var = perf_history[col].var
            
            metrics[col] = [min, max, median, var]
        
        
        # -------------------------------------------
        # 2. Hypothesis Testing
        # -------------------------------------------
        # Null hypothesis: version 4.5.0 perform similar as version 4.4.0
        # Alternative hypothesis: version 4.5.0 perform better than version 4.4.0
        
        
        
        return pd.DataFrame(metrics)
    
    
    def get_comparison(self, perf_reports: list[pd.DataFrame]):
        pass