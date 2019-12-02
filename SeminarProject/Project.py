import requests
from bs4 import BeautifulSoup
import re

#sample_text = 'Alice and Walter are walking to the store.'

#regex = re.compile(r'\.')

#matches = regex.finditer(sample_text)

#for match in matches:
#    print(match)

#sample_text = 'The deep learning class will meet in the third floor to discuss RNN, a subclass of DNN.'
#regex = re.compile(r'class')
#matches = regex.finditer(sample_text)
#for match in matches:
#    print(match)

# Use \b to put boundaries between tokens:
#regex = re.compile(r'\bclass')
#matches = regex.finditer(sample_text)
#for match in matches:
#    print(match)
#regex = re.compile(r'class\b')
#matches = regex.finditer(sample_text)
#for match in matches:
#    print(match)

#regex = re.compile(r'\bclass\b')
#matches = regex.finditer(sample_text)
#for match in matches:
#    print(match)

# Operators: 
# . matches all characters except newline,
# ^ match sequence of characters when appear at beginning of string (ex: r'^match' will match with 'match making....').
# $ match characters when appear at end of string (ex: r'match$' will match with '...rematch').

#sample_text = "this watch is the area's best watch."
#regex_1 = re.compile(r'^this')
#matches_1 = regex_1.finditer(sample_text)
#regex_2 = re.compile(r'watch$')
#matches_2 = regex_2.finditer(sample_text)
#regex_3 = re.compile(r'\bbest\b')
#matches_3 = regex_3.finditer(sample_text)

# Use r (regular expression operator) to ignore escape characters, literal expression. 
# print(r'hello\tclass')

r = requests.get('https://twitter.com/AIForTrading1')

html_data = r.text
print(r.text)

beautifulSoupObj = bs4.BeautifulSoup()

