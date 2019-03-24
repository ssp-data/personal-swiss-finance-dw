path = '/Users/sspaeti/Simon/Sync/Financials/001_Budget/04_Finanzplanung/Transaktionen_Export/'
#filename = 'bekb-viseca-silver-170304_190316.txt'
filename = 'testing.txt'


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
    #print (t)

    print(t['orderDate'])


    #for r in t:
    #    print(r)
    #if len(t)= 5:

