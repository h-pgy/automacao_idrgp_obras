from datetime import date
from typing import Generator, Tuple

def current_year():

    return date.today().year

def current_month():

    return date.today().month

def months_from_today_to(stop_month:int, stop_year:int)->Generator[Tuple[int], Tuple[int],Tuple[int]]:

    if (stop_month>12) or (stop_month<0):
        raise ValueError(f'stop_month be a valid month')
    
    if stop_year<0:
        raise ValueError(f'stop_year be a valid year')

    curr_year = current_year()
    curr_month = current_month()

    year = curr_year
    month = curr_month
    while True:
        yield month, year        
        if year==stop_year and month==stop_month:
            break
        month-=1
        if month==0:
            month=12
            year-=1
