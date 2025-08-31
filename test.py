import datetime

some_date_value = "2023/10/05"
parsed_date = datetime.strptime(some_date_value, "%Y/%m/%d")

print(f"Parsed date: {parsed_date}")
print(f"Year: {parsed_date.year}, Month: {parsed_date.month}, Day: {parsed_date.day}")