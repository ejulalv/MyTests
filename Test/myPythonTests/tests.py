'''
Created on 10 ago. 2018

@author: eJulalv
'''
import xlrd
import pandas as pd



print ("hola fucker !!\n")
df = pd.read_excel('C:\\Users\\ejulalv\\Desktop\\Trabajo\\ejemplos.xlsx',sheet_name='Sheet1')

print (df.sort_values(['Nombre']))

if __name__ == '__main__':
    pass