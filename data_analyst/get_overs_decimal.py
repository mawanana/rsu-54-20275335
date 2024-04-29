# Function to convert string representation of overs to decimal
def convert_to_decimal(overs_str):
    if '.' in overs_str:
        overs, balls = overs_str.split('.')
        decimal_overs = int(overs) + int(balls) / 6
    else:
        decimal_overs = int(overs_str)
    return decimal_overs
