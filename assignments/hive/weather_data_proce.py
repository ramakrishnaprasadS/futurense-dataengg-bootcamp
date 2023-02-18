
# processing spaces separated values in ech line to '#' separated values 

with open('weather_raw.txt', 'r') as file:
    f=open("weather_fine.txt",'a+')
    for line in file:
        line=line.replace(" C","C")
        values=line.split()
        values[1]=values[1][0:4]+"-"+values[1][4:6]+"-"+values[1][6:]
        record="#".join(values)
        f.write(record)
        f.write("\n")
    f.close()


