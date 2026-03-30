import argparse
import pandas as pd

from impala.dbapi import connect


def main():
    conn = connect(host='10.168.49.12', port='21050')
    cursor = conn.cursor()
    
    
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    main()