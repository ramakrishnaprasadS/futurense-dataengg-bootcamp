# import pandas as pd 
# mdf=pd.read_csv("movies.csv")
# m1df=pd.DataFrame({'MovieID':mdf['movieId'],'Title':mdf['title'].apply(lambda x: x[:len(x)-6]),'yoR':mdf['title'].str[-5:-1],'Genres':mdf['genres']})
# m1df['yoR']=m1df['yoR'].apply(lambda x: "Nan" if x.isalpha() or not x.isalnum() else x)

# m1df.to_csv("movies1.csv",index=False)


import pandas as pd 
mdf=pd.read_csv("movies.csv")

mdf['year']=mdf['title'].apply(lambda x: x[-5:-1])
mdf['title']=mdf['title'].apply(lambda x: x[:len(x)-6])

mdf['year']=mdf['year'].apply(lambda x: x if x.isnumeric() else "Nan")

mdf.to_csv("movies.csv",index=False)




