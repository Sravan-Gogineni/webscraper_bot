import pandas as pd 

#read the excel file
df = pd.read_excel('Fields.xlsx')

#print the dataframe
print(df.head())

list_of_fields = df["Column_name"].tolist()

print(list_of_fields)