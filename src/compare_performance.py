import os
import pandas as pd

from scipy import stats


class ComparePerformance:
    def __init__(self, folder: str):
        try:
            self.folder = folder
            self.files = os.listdir(folder)
            
            if len(self.files) > 0:
                valid_shape = None
                
                for file in self.files:
                    perf_history = pd.read_csv(os.path.join(folder, file))
                    
                    if valid_shape is None:
                        valid_shape = perf_history.shape
                    
                    # Compare by shape only — column names differ between
                    # Impala (includes SET MT_DOP prefix) and ClickHouse CSVs
                    assert (perf_history.shape == valid_shape), (
                        f"Shape mismatch in {file}: expected {valid_shape}, got {perf_history.shape}"
                    )

        except FileNotFoundError as e:
            raise
    
        
    def perform_paired_ttest(self, significant_value: float = 0.05):
        '''
        Performance two-sampled paired t-test
        Null hypothesis: 4.5.0 perform just the same as 4.3.0
        Alternative hypothesis: 4.5.0 better than 4.3.0

        Columns are matched by position (not name) because Impala query column
        headers include SET MT_DOP=N; prefixes while ClickHouse headers do not.
        '''
        
        perf_records = []
        for file in self.files:
            perf_records.append(pd.read_csv(os.path.join(self.folder, file)))
                        
        all_col_t_values = []
        all_col_p_values = []
        all_col_result = []
        result_description = []
        query_labels = []

        num_cols = len(perf_records[0].columns)

        for i in range(num_cols):
            # Match by position — avoids KeyError when column names differ
            sample_a = perf_records[0].iloc[:, i]
            sample_b = perf_records[1].iloc[:, i]

            # Use the shorter/cleaner of the two column names as the label
            col_a = perf_records[0].columns[i]
            col_b = perf_records[1].columns[i]
            label = col_a if len(col_a) <= len(col_b) else col_b
            query_labels.append(label)
            
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
            'sample_a': [self.files[0]] * num_cols,
            'sample_b': [self.files[1]] * num_cols,
            'query': query_labels,
            't_value': all_col_t_values,
            'p_value': all_col_p_values,
            'is_reject_H0': all_col_result,
            'description': result_description 
        }), perf_records[0], perf_records[1], self.files[0], self.files[1]