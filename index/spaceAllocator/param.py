import argparse

parser = argparse.ArgumentParser(description='IndexOnLake')

# -- learned index --
parser.add_argument('--segment_error', type=int, default=100,
                    help='error for Fiting index')

parser.add_argument('--sieve_gap_percent', type=float, default=0.01,
                    help="sieve gap percent")

parser.add_argument('--largegapth', type=int, default=10,
                    help='largegapth')

args = parser.parse_args()