import os, sys

from enrich.customers import get_customers_in_dir

def get_customers():
    thisdir = os.path.abspath(os.path.dirname(__file__))
    customers = get_customers_in_dir(thisdir)
    return customers

