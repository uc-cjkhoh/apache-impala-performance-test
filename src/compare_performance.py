import os
import pandas as pd

from scipy import stats


class ComparePerformance:
    def __init__(self, folder: str):
        try:
            self.folder = folder
            self.files = os.listdir(folder)
            
            if len(self.files) > 0:
                valid_column = None
                valid_shape = None
                
                for file in self.files:
                    perf_history = pd.read_csv(os.path.join(folder, file))
                    
                    if valid_column is None:
                        valid_column = perf_history.columns
                    
                    if valid_shape is None:
                        valid_shape = perf_history.shape
                    
                    # assert (perf_history.columns == valid_column).all()
                    assert (perf_history.shape == valid_shape)

        except FileNotFoundError as e:
            raise
    
        
    def perform_paired_ttest(self, significant_value: float = 0.05):
        '''
        Performance two-sampled paired t-test
        Null hypothesis: 4.5.0 perform just the same as 4.3.0
        Alternative hypothesis: 4.5.0 better than 4.3.0
        '''
        
        perf_records = []
        for file in self.files:
            perf_records.append(pd.read_csv(os.path.join(self.folder, file)))
                        
        all_col_t_values = []
        all_col_p_values = []
        all_col_result = []
        result_description = []
        for col in perf_records[0].columns:
            sample_a = perf_records[0][col]
            sample_b = perf_records[1][col]
            
            result = stats.ttest_rel(sample_a, sample_b)
            
            if result.pvalue < significant_value:
                all_col_result.append(1)
                
                if result.statistic > 0:
                    result_description.append('Sample A is slower than Sample B')
                
                else:
                    result_description.append('Sample A is faster than Sample B')
                    
            else:
                all_col_result.append(0)
                result_description.append('Both samples has no significant difference')

            all_col_t_values.append(result.statistic)
            all_col_p_values.append(result.pvalue)
            
            
        return pd.DataFrame({
            'sample_a': [self.files[0]] * len(perf_records[0].columns),
            'sample_b': [self.files[1]] * len(perf_records[0].columns),
            'query': perf_records[0].columns,
            't_value': all_col_t_values,
            'p_value': all_col_p_values,
            'is_reject_H0': all_col_result,
            'description': result_description 
        }), perf_records[0], perf_records[1], self.files[0], self.files[1]  # Return raw data and sample names
    
     