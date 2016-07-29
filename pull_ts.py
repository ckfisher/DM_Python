import numpy as np 
import pandas as pd 
import grads 
import matplotlib.pyplot as plt

grads_exe='grads'
ga = grads.GrADS(Bin=grads_exe,Window=False,Echo=False)

#Open the two varibales of interest from the drought monitor
ga('sdfopen http://stream.princeton.edu:9090/dods/LAFDM/ETO/DAILY')
ga('sdfopen http://stream.princeton.edu:9090/dods/LAFDM/3B42RT_BC/DAILY')

#Choose the cell we want to look at
ga('set lat -7')
ga('set lon -44')
ga('set time 00z01jan2014 00z01jan2016')

#Export the time series
#Note this is ga.expr() for time series not ga.exp()
eto = np.ma.getdata(ga.expr('eto.1'))
prec = np.ma.getdata(ga.expr('prec.2'))

#Use Pandas to do a moving 7 day average
t = pd.date_range('1/1/2014','1/1/2016',freq='D')

eto_s = pd.Series(eto,t)
prec_s = pd.Series(prec,t)

eto_ma = pd.rolling_mean(eto_s,7)
prec_ma = pd.rolling_mean(prec_s,7)

#Plot the data
plt.figure()
ax = eto_ma.plot(label='ETo')
prec_ma.plot(ax=ax, label='Precip')
plt.legend()
plt.show()
