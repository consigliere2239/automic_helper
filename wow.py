import nltk
#nltk.download('words')
from nltk.corpus import words
from itertools import permutations
from datetime import datetime
import string
def sort_strings_by_length(strings):
   
   return sorted(strings, key=len, reverse=True)
def generate_words(letters):
   
   distinctive=[]
   all_combinations = set()
   
   for r in range(3, len(letters) + 1):
       for combo in permutations(letters, r):
           all_combinations.add(''.join(combo))
   
   for word in sorted(all_combinations):
       
       if word not in distinctive:
           distinctive.append(word)
       
   return distinctive

start_time=datetime.now()
dosya_adi='output.txt'
with open(dosya_adi, 'w') as dosya:
   # str=''
   print('hafleri boşluk bırakmadan gir')
   harfler=input(str())
   print('lütfen bekle')
   letters = list(harfler)
   print(letters, 'harfleri için uygun kelimeler aranıyor')
   lst=[]
   for x in generate_words(letters):
       if x in words.words():
           if x not in lst:
               lst.append(x)
   for i in (sort_strings_by_length(lst)):
       ww=len(i)
       www=str(ww)
       dosya.write(i + '\t' + str(ww) + '\n')
   end_time=datetime.now()
   duration=end_time-start_time
   print(start_time)
   print(end_time)
   print(duration)