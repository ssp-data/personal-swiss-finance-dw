import csv


path = '/Users/sspaeti/Simon/Sync/Financials/001_Budget/04_Finanzplanung/Transaktionen_Export/'
#filename = 'bekb-viseca-silver-170304_190316.txt'
filename = 'clerc-viseca-181001_190131_mary.txt'
#filename = 'testing.txt'


l = []
trans = {}
transactions = []
lines = tuple(open(path + filename, 'r'))

i = 0
for line in lines:
    if line.strip() in('JANUAR', 'FEBRUAR', 'MÄRZ', 'APRIL', 'MAI', 'JUNI', 'JULI', 'AUGUST', 'SEPTEMBER', 'OKTOBER', 'NOVEMBER', 'DEZEMBER'):
        continue
    if line[0] == ' ' and bool(trans):
        #transactions.append(l)
        transactions.append(trans.copy())
        #l = []
        trans = {}
        i = 0

    if i == 0:
        dictName = 'Category'
    elif i == 1:
        dictName = 'Payee'
    elif i == 2:
        dictName = 'orderDate'
    elif i == 3:
        dictName = 'amountCHF'
    elif i == 4:
        dictName = 'amountForeign'

    trans[dictName] = line.strip()
    #l.append({dictName : line.strip()})
    i += 1

#print(transactions)

for t in transactions:
    t['Payee'] = t['Payee'] + ' ' + t['orderDate'][17:]
    t['orderTime'] = t['orderDate'][11:16]
    t['orderDate'] = t['orderDate'][:10]

i=0
for t in transactions:
    if len(t) == 6:
        break
    i += 1

with open(path + filename+'.csv', 'w') as f:  # Just use 'w' mode in 3.x
    w = csv.DictWriter(f, transactions[i].keys())
    w.writeheader()
    w.writerows(transactions)

