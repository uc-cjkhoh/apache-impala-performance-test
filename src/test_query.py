from dataclasses import dataclass, field


@dataclass
class QueryList:
    queries: list = field(default_factory=list)
    
    def add(self, query: str):
        self.queries.append(query)
            
    
def get_queries():
    querylist = QueryList()
     
    # WHERE statement
    # 1. Simple filter on partition columns
    querylist.add(
        ''' 
        SELECT 
            trans_iid, msisdn, imsi, op_code, status, log_dt
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year = 2024 AND par_month = 6 AND par_date = 5 AND bound_type = 0
        LIMIT 
            1000;
        '''
    )
    
    # 2. Multi-condition filter with non-partition columns
    querylist.add(
        ''' 
        SELECT 
            msisdn, imsi, mcc, mnc, rat_type, status
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year = 2024
            AND par_month = 6
            AND op_code IN (2, 3)
            AND status = 'SUCCESS'
            AND msisdn != 0
            AND mcc BETWEEN 500 AND 520;
        '''
    )
     
    # GROUP BY statement
    # 1. Roaming traffic volume by operator
    querylist.add(
        '''
        SELECT 
            mcc, mnc, op_code, COUNT(*) AS total_tx, COUNT(DISTINCT imsi) AS unique_subs
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year = 2024
            AND par_month = 6
        GROUP BY 
            mcc, mnc, op_code 
        '''
    )
     
    # WINDOW clause 
    # 1. Running total of transactions per hour within a day
    querylist.add(
        '''
        SELECT tx_date, tx_hour, tx_count,
            SUM(tx_count) OVER (
                PARTITION BY tx_date
                ORDER BY tx_hour
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS running_total
        FROM (
            SELECT 
                tx_date, tx_hour, COUNT(*) AS tx_count
            FROM 
                roam352_report_digi.data_em
            WHERE 
                par_year = 2024
                AND par_month = 6
                AND par_date = 5
            GROUP BY 
                tx_date, tx_hour
        ) t
        ORDER BY 
            tx_date, tx_hour;
        '''
    )
    
    # JOIN statement  
    # 1. Per-hour roaming tx share vs total, with running cumulative and rank
    querylist.add(
        ''' 
        SELECT
            hour_stats.tx_hour,
            hour_stats.mcc,
            hour_stats.roaming_tx,
            hour_stats.total_tx,
            ROUND(100.0 * hour_stats.roaming_tx / NULLIF(hour_stats.total_tx, 0), 2) AS roaming_pct,
            SUM(hour_stats.roaming_tx) OVER (
                PARTITION BY hour_stats.mcc
                ORDER BY hour_stats.tx_hour
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_roaming,
            RANK() OVER (
                PARTITION BY hour_stats.tx_hour
                ORDER BY hour_stats.roaming_tx DESC
            ) AS roaming_rank_per_hour
        FROM (
            SELECT a.tx_hour,
                    a.mcc,
                    COUNT(*) AS total_tx,
                    SUM(CASE WHEN a.mcc != b.home_mcc THEN 1 ELSE 0 END) AS roaming_tx
            FROM roam352_report_digi.data_em a
            JOIN (
                SELECT DISTINCT msisdn, mcc_ref AS home_mcc
                FROM roam352_report_digi.data_em
                WHERE par_year = 2024 AND par_month = 6
                AND msisdn != 0
            ) b ON a.msisdn = b.msisdn
            WHERE a.par_year  = 2024
                AND a.par_month = 6
                AND a.par_date  = 5
                AND a.msisdn   != 0
                AND a.status    = 'SUCCESS'
            GROUP BY a.tx_hour, a.mcc
        ) hour_stats
        ORDER BY hour_stats.mcc, hour_stats.tx_hour;
        '''
    )
    
    
    return querylist.queries