from dataclasses import dataclass, field


@dataclass
class QueryList:
    queries: list = field(default_factory=list)
    
    def add(self, query: str):
        self.queries.append(query)
            
    
def get_queries():
    querylist = QueryList()
    
    # Query 1: SELECT statement
    querylist.add('SELECT * FROM roam352_report_digi.data_em LIMIT 10000')
    
    # Query 2: WHERE statement
    
    # Query 3: GROUP BY statement
    
    # Query 4: WINDOW clause
    
    # Query 5: JOIN statement
    
    return querylist.queries