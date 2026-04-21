from dataclasses import dataclass, field


@dataclass
class QueryList:
    queries: list = field(default_factory=list)
    mt_dop: list = field(default_factory=list)
    
    def add(self, query: str, mt_dop: int = 0):
        self.queries.append(query)
        self.mt_dop.append(mt_dop)
          

def sql_improvement(ql: QueryList):
    # --------------------------------------------------------------------------
    # IMPALA 4.4.0 Improvement
    # --------------------------------------------------------------------------
     
    # 1. [4.4.0 | IMPALA-11123] COUNT(*) Footer Optimization for ORC
    # -- Test 1a: Simple COUNT(*) — should use ORC footer metadata only
    ql.add(
        '''
        SELECT 
            COUNT(*)
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year = 2024
            AND par_month = 202401;
        '''
    )
    
    # -- Test 1b: COUNT(*) with GROUP BY — partial footer optimization
    ql.add(
        '''
        SELECT 
            status, COUNT(*)
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year = 2024
            AND par_month = 202401
        GROUP BY 
            status;
        '''
    )
    
    # -- Test 1c: COUNT(*) per op_code — compare with full scan baseline
    ql.add(
        '''
        SELECT 
            op_code, COUNT(*)
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year = 2024
        GROUP BY 
            op_code
        ORDER BY 
            op_code;
        '''
    )
    
    
    # 2. [4.4.0 | IMPALA-12631] COUNT(*) Parquet Footer Optimization with MT_DOP
    # -- Test 2a: COUNT(*) with MT_DOP=0 (baseline) 
    
    ql.add(
        ''' 
        SELECT 
            COUNT(*)
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year = 2024
            AND par_month = 202401;
        '''
    )
    
    # -- Test 2b: COUNT(*) with MT_DOP=4 (should now use footer-only path)
    ql.add(
        ''' 
        SELECT 
            COUNT(*)
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year = 2024
            AND par_month = 202401;
        ''',
        mt_dop=4
    )
    
    # -- Test 2c: COUNT(*) with partition filter + MT_DOP
    ql.add(
        '''
        SELECT 
            COUNT(*)
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year  = 2024
            AND par_month = 202401
            AND par_date = 20240101;
        ''',
        mt_dop=4
    )
    
    
    # 3. [4.4.0 | IMPALA-3825] Distributed Runtime Filter Aggregation
    # -- Test 3a: Large join with runtime filter on high-cardinality key
    # --          Filter on msisdn pushes down into scan via Bloom filter
    ql.add(
        '''
        SELECT a.msisdn,
            a.mcc,
            a.mnc,
            COUNT(*)                                                    AS total_tx,
            SUM(CASE WHEN a.status = 'SUCCESS' THEN 1 ELSE 0 END)      AS success_cnt
        FROM roam352_report_digi.data_em a
        JOIN (
            -- Simulates a smaller dimension-like subset (distinct roaming home operators)
            SELECT DISTINCT msisdn
            FROM roam352_report_digi.data_em
            WHERE par_year  = 2024
            AND par_month = 202401
            AND par_date = 20240101
            AND op_code   = 3
            AND mcc != mcc_ref
        ) b ON a.msisdn = b.msisdn
        WHERE a.par_year  = 2024
        AND a.par_month = 202401
        GROUP BY a.msisdn, a.mcc, a.mnc
        ORDER BY total_tx DESC
        LIMIT 100;
        '''
    )
    
    # -- Test 3b: Multi-column runtime filter — join on mcc + op_code
    ql.add(
        '''
        SELECT a.tx_hour,
            a.mcc,
            a.op_code,
            COUNT(*)        AS tx_count,
            COUNT(DISTINCT a.msisdn) AS unique_subs
        FROM roam352_report_digi.data_em a
        JOIN (
            SELECT DISTINCT mcc, op_code
            FROM roam352_report_digi.data_em
            WHERE par_year  = 2024
            AND par_month = 202401 
            AND par_date = 20240101 
            AND mcc BETWEEN 500 AND 530
        ) b ON a.mcc = b.mcc AND a.op_code = b.op_code
        WHERE a.par_year  = 2024
        AND a.par_month = 202401
        AND a.par_date = 20240101
        GROUP BY a.tx_hour, a.mcc, a.op_code
        ORDER BY a.tx_hour, a.mcc;
        ''' 
    ) 
    
    
    # 4. [4.4.0 | IMPALA-12881] FK-PK Join Cardinality Estimation  
    # -- Test 4a: FK-PK join with aggregation — check memory allocation accuracy
    ql.add(
        '''
        SELECT sub.subs_type,
            sub.rate_plan,
            COUNT(*) AS total_tx,
            COUNT(DISTINCT t.msisdn) AS unique_subs,
            SUM(CASE WHEN t.status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_cnt
        FROM roam352_report_digi.data_em t
        JOIN (
            SELECT DISTINCT msisdn, subs_type, rate_plan
            FROM roam352_report_digi.data_em
            WHERE par_year  = 2024
            AND par_month = 202401
            AND par_date = 20240101
            AND subs_type IS NOT NULL
        ) sub ON t.msisdn = sub.msisdn
        WHERE t.par_year  = 2024
        AND t.par_month = 202401
        AND t.par_date = 20240101
        GROUP BY sub.subs_type, sub.rate_plan
        ORDER BY total_tx DESC;
        '''
    )
     
     
     
    # -----------------------------------------------------------------------
    # IMPALA IMPROVEMENT 4.5.0
    # -----------------------------------------------------------------------
    
    # 1. [4.5.0 | IMPALA-889] ANSI SQL TRIM() Function Support
    # -- Test 5a: TRIM LEADING zeros from trans_iid (hex transaction ID)
    # ql.add(
    #     '''
    #     SELECT 
    #         trans_iid, TRIM(LEADING '0' FROM trans_iid) AS trimmed_trans_iid
    #     FROM 
    #         roam352_report_digi.data_em
    #     WHERE 
    #         par_year  = 2024
    #         AND par_month = 202401
    #         AND par_date BETWEEN 20240101 AND 20240115
    #     LIMIT 20;
    #     '''
    # )
    
    # # -- Test 5b: TRIM TRAILING spaces from varchar fields
    # ql.add(
    #     '''
    #     SELECT 
    #         msisdn,
    #         rate_plan,
    #         TRIM(TRAILING ' ' FROM rate_plan) AS clean_rate_plan,
    #         lang_code,
    #         TRIM(BOTH ' ' FROM lang_code) AS clean_lang_code
    #     FROM 
    #         roam352_report_digi.data_em
    #     WHERE 
    #         par_year  = 2024
    #         AND par_month = 202401
    #         AND rate_plan IS NOT NULL
    #     LIMIT 20;
    #     '''
    # )
    
    # # -- Test 5c: TRIM in WHERE predicate — filter on trimmed value
    # ql.add(
    #     '''
    #     SELECT 
    #         COUNT(*) AS cnt
    #     FROM 
    #         roam352_report_digi.data_em
    #     WHERE 
    #         par_year  = 2024
    #         AND par_month = 202401
    #         AND TRIM(BOTH ' ' FROM subs_type) = 'PREPAID';
    #     '''
    # )
    
    # # -- Test 5d: Aggregate using TRIM-normalized grouping key
    # ql.add(
    #     '''
    #     SELECT 
    #         TRIM(BOTH ' ' FROM subs_type) AS subs_type_clean,
    #         TRIM(BOTH ' ' FROM rate_plan) AS rate_plan_clean,
    #         COUNT(*) AS tx_count,
    #         COUNT(DISTINCT msisdn) AS unique_subs
    #     FROM 
    #         roam352_report_digi.data_em
    #     WHERE 
    #         par_year  = 2024
    #         AND par_month = 202401
    #         AND subs_type IS NOT NULL
    #     GROUP BY 
    #         TRIM(BOTH ' ' FROM subs_type),
    #         TRIM(BOTH ' ' FROM rate_plan)
    #     ORDER BY 
    #         tx_count DESC;
    #     '''
    # )
    
    
    # 2. [4.5.0 | IMPALA-2945] Pre-Aggregation Cardinality Estimation
    # -- Test 6a: Low-cardinality GROUP BY — status has very few distinct values
    ql.add(
        '''
        SELECT 
            status, COUNT(*) AS tx_count
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year = 2024
            AND par_month = 202401
        GROUP BY 
            status;
        '''
    )
    
    # -- Test 6b: Low-cardinality multi-column GROUP BY — few distinct combos
    ql.add(
        '''
        SELECT 
            op_code, bound_type, status,
            COUNT(*)                   AS total_tx,
            COUNT(DISTINCT msisdn)     AS unique_subs,
            COUNT(DISTINCT mcc)        AS unique_visited_mcc
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year = 2024
            AND par_month = 202401
            AND par_date = 20240101
        GROUP BY 
            op_code, bound_type, status
        ORDER BY 
            total_tx DESC;
        '''
    )
    
    # -- Test 6c: Low-cardinality GROUP BY on large date range
    # --          Planner should reduce parallelism since few distinct groups expected
    ql.add(
        '''
        SELECT 
            tx_month, tx_week, op_code, status,
            COUNT(*)              AS tx_count,
            SUM(CASE WHEN mcc != mcc_ref THEN 1 ELSE 0 END) AS roaming_cnt
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year = 2024
            AND par_month = 202401 
        GROUP BY 
            tx_month, tx_week, op_code, status
        ORDER BY 
            tx_month, tx_week;
        '''
    )
    
    
    # 3. [4.5.0 | IMPALA-8042] Better Selectivity Estimate for BETWEEN
    # -- Test 7a: BETWEEN on tx_hour — narrow range (high selectivity)
    ql.add(
        '''
        SELECT tx_hour, op_code, COUNT(*) AS tx_count
        FROM roam352_report_digi.data_em
        WHERE par_year  = 2024
        AND par_month = 202401
        AND tx_hour BETWEEN 8 AND 10   -- narrow: ~12.5% of hours
        GROUP BY tx_hour, op_code
        ORDER BY tx_hour;
        '''
    )
    
    # -- Test 7b: BETWEEN on module_dt — used in join predicate
    # --          Optimizer should pick better join strategy with correct selectivity
    ql.add(
        '''
        SELECT a.msisdn,
            a.mcc,
            a.op_code,
            COUNT(*) AS tx_count
        FROM roam352_report_digi.data_em a
        JOIN (
            SELECT DISTINCT msisdn
            FROM roam352_report_digi.data_em
            WHERE par_year   = 2024
            AND par_month  = 202401
        ) narrow ON a.msisdn = narrow.msisdn
        WHERE a.par_year  = 2024
        AND a.par_month = 202401
        GROUP BY a.msisdn, a.mcc, a.op_code
        ORDER BY tx_count DESC
        LIMIT 50;
        '''
    )  
    
    # 4. [4.5.0 | IMPALA-12800] Nested Inline Views — ExprSubstitutionMap Fix
    # -- Test 8a: Deeply nested inline views (6 levels)
    # --          Tests O(n²) → O(n log n) ExprSubstitutionMap fix
    ql.add(
        '''
        SELECT mcc, rank_in_mcc, COUNT(*) AS sub_count
        FROM (
            SELECT msisdn, mcc, mnc, pct_roaming, rank_in_mcc
            FROM (
                SELECT msisdn, mcc, mnc, pct_roaming,
                    RANK() OVER (PARTITION BY mcc ORDER BY pct_roaming DESC) AS rank_in_mcc
                FROM (
                    SELECT msisdn, mcc, mnc,
                        roaming_cnt * 100.0 / NULLIF(total_tx, 0) AS pct_roaming
                    FROM (
                        SELECT msisdn, mcc, mnc,
                            total_tx,
                            roaming_cnt,
                            total_tx - roaming_cnt AS non_roaming_cnt
                        FROM (
                            SELECT msisdn, mcc, mnc,
                                COUNT(*)                                         AS total_tx,
                                SUM(CASE WHEN mcc != mcc_ref THEN 1 ELSE 0 END) AS roaming_cnt
                            FROM (
                                SELECT msisdn, mcc, mnc, mcc_ref, op_code, status
                                FROM (
                                    SELECT msisdn, mcc, mnc, mcc_ref,
                                        op_code, status, bound_type
                                    FROM (
                                        SELECT msisdn, mcc, mnc, mcc_ref,
                                            op_code, status, bound_type, vlr
                                        FROM roam352_report_digi.data_em
                                        WHERE par_year  = 2024
                                        AND par_month = 202401
                                        AND par_date  = 20240101
                                        AND msisdn   != 0
                                    ) s1
                                    WHERE bound_type IN (0, 1)
                                ) s2
                                WHERE op_code IN (2, 3)
                            ) s3
                            GROUP BY msisdn, mcc, mnc, mcc_ref
                        ) s4
                    ) s5
                ) s6
            ) s7
            WHERE rank_in_mcc <= 10
        ) s8
        GROUP BY mcc, rank_in_mcc
        ORDER BY mcc, rank_in_mcc;
        '''
    )
    
    # -- Test 8b: Extended nesting with window + aggregation (8 levels)
    ql.add(
        '''
        SELECT final.*
        FROM (
            SELECT msisdn, tx_hour, tx_count, running_total,
                ROUND(running_total * 100.0 / NULLIF(day_total, 0), 2) AS cumulative_pct
            FROM (
                SELECT msisdn, tx_hour, tx_count,
                    SUM(tx_count) OVER (PARTITION BY msisdn ORDER BY tx_hour) AS running_total,
                    SUM(tx_count) OVER (PARTITION BY msisdn)                  AS day_total
                FROM (
                    SELECT msisdn, tx_hour, COUNT(*) AS tx_count
                    FROM (
                        SELECT msisdn, tx_hour, op_code, status
                        FROM (
                            SELECT *
                            FROM roam352_report_digi.data_em
                            WHERE par_year  = 2024
                            AND par_month = 202401
                            AND par_date = 20240101
                        ) base
                        WHERE op_code IN (2, 3) 
                    ) filtered
                    GROUP BY msisdn, tx_hour
                ) hourly
            ) windowed
        ) final
        WHERE msisdn != 0
        ORDER BY msisdn, tx_hour
        LIMIT 200;
        '''
    )
    
    
    # 5. [4.5.0 | IMPALA-13658] Cache Aggregate Results Below Exchange
    # -- Test 9a: Run FIRST time (populates cache below exchange)
    ql.add(
        '''
        SELECT 
            mcc, op_code, COUNT(*) AS tx_count, COUNT(DISTINCT msisdn) AS unique_subs
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year  = 2024
            AND par_month = 202401 
        GROUP BY 
            mcc, op_code;
        '''
    )
    
    # -- Test 9b: Run SECOND time (same scan range — should reuse cached pre-agg)
    ql.add(
        '''
        SELECT 
            mcc, op_code, SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_cnt
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year  = 2024
            AND par_month = 202401
            AND par_date = 20240101
        GROUP BY 
            mcc, op_code;
        '''
    )
    
    # -- Test 9c: Third query — different aggregation, same partition
    ql.add(
        '''
        SELECT mcc, bound_type,
            COUNT(*)               AS total_tx,
            COUNT(DISTINCT imsi)   AS unique_imsi,
            COUNT(DISTINCT vlr)    AS unique_vlr
        FROM 
            roam352_report_digi.data_em
        WHERE 
            par_year  = 2024
            AND par_month = 202401 
            AND par_date = 20240101
        GROUP BY 
            mcc, bound_type
        ORDER BY 
            total_tx DESC;
        '''
    )
    
    
    # 6. [4.5.0 | IMPALA-13405 + IMPALA-13465] AggregationNode Cardinality — Predicate Reduction + TupleId Tracing
    # -- Test 10a: Single-level predicate reduction on GROUP BY column
    # --           Only 1 op_code value → aggregation cardinality = 1, not NDV(op_code)
    ql.add(
        '''
        SELECT op_code,
            tx_hour,
            COUNT(*)                AS total_tx,
            COUNT(DISTINCT msisdn)  AS unique_subs
        FROM roam352_report_digi.data_em
        WHERE par_year  = 2024
        AND par_month = 202401
        AND op_code   = 2           -- single value predicate → agg cardinality reduced
        GROUP BY op_code, tx_hour
        ORDER BY tx_hour;
        '''
    )
    
    # -- Test 10b: IN-list predicate — 2 of N possible bound_type values
    ql.add(
        '''
        SELECT bound_type, status,
            COUNT(*)              AS tx_count,
            COUNT(DISTINCT mcc)   AS unique_mcc
        FROM roam352_report_digi.data_em
        WHERE par_year  = 2024
        AND par_month = 202401
        AND bound_type IN (0, 1)   -- subset of all bound_type values
        GROUP BY bound_type, status;
        '''
    )
    
    # -- Test 10c: TupleId tracing — multi-join, filter on dim-side tuple
    # --           4.5.0 traces predicate on mcc_ref across join to tighten agg estimate
    ql.add(
        '''
        SELECT visited.mcc,
            home.mcc_ref,
            COUNT(*)                       AS tx_count,
            COUNT(DISTINCT visited.msisdn) AS unique_subs
        FROM roam352_report_digi.data_em visited
        JOIN (
            SELECT DISTINCT msisdn, mcc_ref
            FROM roam352_report_digi.data_em
            WHERE par_year  = 2024
            AND par_month = 202401
            AND mcc_ref = 502
        ) home ON visited.msisdn = home.msisdn
        WHERE visited.par_year  = 2024
        AND visited.par_month = 202401
        GROUP BY visited.mcc, home.mcc_ref;
        '''
    )
    
    
    # 7. [4.5.0 | IMPALA-13509] Avoid Duplicate DeepCopy in KrpcDataStreamSender
    # -- Test 11a: High-cardinality distributed GROUP BY — triggers large shuffle
    ql.add(
        ''' 
        SELECT msisdn,
            imsi,
            COUNT(*)                AS total_tx,
            COUNT(DISTINCT vlr)     AS vlr_hops,
            COUNT(DISTINCT mcc)     AS mcc_visited,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)      AS success_cnt,
            SUM(CASE WHEN mcc != mcc_ref     THEN 1 ELSE 0 END)      AS roaming_tx
        FROM roam352_report_digi.data_em
        WHERE 
            par_year  = 2024
            AND par_month = 202401
            AND par_date = 20240101
            AND msisdn   != 0
        GROUP BY msisdn, imsi
        ORDER BY total_tx DESC
        LIMIT 500; 
        ''',
        mt_dop=4
    )
    
    # -- Test 11b: Large distributed join — forces hash-partition exchange on both sides
    ql.add(
        '''  
        SELECT a.msisdn,
            a.mcc,
            a.vlr,
            b.op_code AS paired_op_code,
            COUNT(*) AS pair_count
        FROM roam352_report_digi.data_em a
        JOIN roam352_report_digi.data_em b
            ON a.msisdn = b.msisdn
            AND a.par_year = b.par_year
            AND a.par_month = b.par_month 
            AND a.op_code != b.op_code
        WHERE 
            a.par_year = 2024
            AND a.par_month = 202401
            AND a.par_date = 20240101
            AND b.par_year = 2024
            AND b.par_month = 202401
            AND b.par_date = 20240101
            AND a.op_code IN (23, 316)
            AND b.op_code IN (23, 316)
            AND a.mcc IN (502, 510)
            AND b.mcc IN (502, 510)
            AND a.msisdn != 0
            AND b.msisdn != 0
        GROUP BY a.msisdn, a.mcc, a.vlr, b.op_code 
        LIMIT 200; 
        ''',
        mt_dop=4
    )
    
    return ql
     
    
def get_queries():  
    ql = QueryList()
     
    return [sql_improvement(ql)]
