import requests as re
import pandas as pd

search = "ferre"

url = f"https://search.openfoodfacts.org/search?q={search}&page_size=10&page=1&fields=product_name%2Clang%2Cbrands%2Cnutrition_grades%2Cecoscore_grade"

df = pd.DataFrame(re.get(url).json().get('hits'))

print(df)
