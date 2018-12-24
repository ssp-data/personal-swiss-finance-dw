import codecs

path = '/Users/sspaeti/Simon/Sync/Financials/002_Bank/2_Bekb/export/'
file = 'lohn_mt940.kto'

with codecs.open(path + file, 'r', encoding='utf8') as f:
    text = f.read()



f = open(path + file, "r") 

print(f.read(5))


