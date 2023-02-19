with open('transactions.txt', 'r') as file:
    f=open("transactions_fine.txt",'a+')
    for line in file:
        values=line.split(',')
        dt=values[1].split('-')
        values[1]=dt[2]+"-"+dt[0]+"-"+dt[1]
        record=",".join(values)
        f.write(record)
        #f.write("\n")
    f.close()