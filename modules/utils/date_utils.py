from datetime import date
from dateutil.relativedelta import relativedelta

def get_target_yyyymm(months_ago=2):
    """
    Returns the year-month string (yyyy-MM) for the given number of months ago.
    """
    target_date = date.today() - relativedelta(months=months_ago)
    return target_date.strftime("%Y-%m")


def get_month_start_n_months_ago(months_ago: int = 2) -> date:
    """
    Returns the date representing the first day of the month, 'n' months ago.

    Parameters:
        months_ago (int): Number of months to go back from today. Default is 2.

    Returns:
        date: A date object set to the first day of the target month.
    """
    return date.today().replace(day=1) - relativedelta(months=months_ago)