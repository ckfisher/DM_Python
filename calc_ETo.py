import datetime 
import matplotlib
matplotlib.use('agg')
import numpy as np 
import grads 
import dateutil.relativedelta as relativedelta
import matplotlib.pyplot as plt
import multiprocessing as mp


grads_exe = 'grads'

def datetime2gradstime(date):

   #Convert datetime to grads time
   str = date.strftime('%HZ%d%b%Y')

   return str

def gradstime2datetime(str):

    #Convert grads time to datetime
    date = datetime.datetime.strptime(str,'%HZ%d%b%Y')

    return date

def Extract_Data_Period_Average(idate_out,fdate_out,dt_down,dt_up,dt,ctl_in,var,type,open_type,forecast):
 #Start up the grads instance
 ga = grads.GrADS(Bin=grads_exe,Window=False,Echo=False)
 #Open access to the control file
 ga("%s %s" % (open_type,ctl_in))
 #ga.open(ctl_in)

 #Determine initial and final time step
 ga('set t 1')
 idate_all = gradstime2datetime(ga.exp(var).grid.time[0])
 ga('set t last')
 fdate_all = gradstime2datetime(ga.exp(var).grid.time[0])

 date = idate_out
 count = 0
 buffer = 0
 while date <= fdate_out:

  date1 = date - dt_down
  date2 = date + dt_up
  #If we are not doing a calculation that extends beyond the ctl file limits
  if forecast == False:
    if date1 < idate_all and date2 < idate_all:
      date = date + dt
      continue
    if date1 < idate_all and date2 >= idate_all:
      date1 = idate_all
    if date1 > fdate_all and date2 > fdate_all:
      date = date +dt
      continue
    if date1 <= fdate_all and date2 > fdate_all:
      date2 = fdate_all
  #Time conditions for the case of seasonal forecast months
  else:
    #Determine the number of NaN timesteps which must be returned 
    if datetime.datetime(date2.year,date2.month,1) > fdate_all:
      buffer = (date2.year - fdate_all.year)*12 + date2.month - fdate_all.month
      date2 = fdate_all
      if date1 >= date2:
        buffer = buffer + (date2.year - date1.year)*12 + date2.month - date1.month
        date1 = date2

  t1 = datetime2gradstime(date1)
  t2 = datetime2gradstime(date2)

  #Extract data
  if type == "ave":
   ga("data = ave(%s,time=%s,time=%s)" % (var,t1,t2))
   tmp = np.ma.getdata(ga.exp("maskout(data,data)"))
 
  if type == "all":
   if t1 == t2:
    ga("set time %s" % t1)
   else:
    ga("set time %s %s" % (t1,t2))
   tmp = np.ma.getdata(ga.exp("maskout(%s,%s)" % (var,var)))

  #Convert 2-d arrays to 3-d for stacking
  if len(tmp.shape) == 2:
   tmp = np.reshape(tmp,(1,tmp.shape[0],tmp.shape[1]))

  #Append data
  if count == 0:#date == idate_out:
   count = 1
   data = tmp
   #Write NaN arrays for extra timesteps
   if buffer > 0:
     tmp2 = np.empty([buffer,tmp.shape[1],tmp.shape[2]])
     tmp2[:] = np.NAN
     data = np.vstack((data,tmp2))
  else:
   data = np.vstack((data,tmp))

  #Update time step
  date = date + dt 
 
 #Close access to the grads data
 ga("close 1")
 return data



def Calc_ETo(date):
  #Read in all data from the correct files
  #rnet - W/m2
  #prec - mm/day
  #tmax - K
  #tmin - K
  #wind - m/s

  print date
  ga = grads.GrADS(Bin=grads_exe,Window=False,Echo=False)

  #Setting time variables for the data extraction
  idate_tstep = date
  fdate_tstep = date
  dt_up = relativedelta.relativedelta(days=0)
  dt_down = relativedelta.relativedelta(days=0)
  type = 'all'
  
  #Read in the continent mask for use later on 
  mask_file = 'http://stream.princeton.edu:9090/dods/LAFDM/MASK'
  ga("sdfopen %s" % mask_file)
  ga('set t 1')
  mask = np.ma.getdata(ga.exp("maskout(%s,%s)" % ("mask","mask")))
  mask[mask<0] = np.nan
  ga("close 1")

  #If the data is before 2003 use PGF as the forcings
  if date < datetime.datetime(2003,1,1):
    vic_file = 'http://stream.princeton.edu:9090/dods/LAFDM/VIC_PGF/DAILY'
    forcing_file = 'http://stream.princeton.edu:9090/dods/LAFDM/PGF/DAILY'
    r_net = np.squeeze(Extract_Data_Period_Average(idate_tstep,fdate_tstep,dt_down,dt_up,dt,vic_file,"r_net",type,"sdfopen",False))
    tmin = np.squeeze(Extract_Data_Period_Average(idate_tstep,fdate_tstep,dt_down,dt_up,dt,forcing_file,"tmin",type,"sdfopen",False))
    tmax = np.squeeze(Extract_Data_Period_Average(idate_tstep,fdate_tstep,dt_down,dt_up,dt,forcing_file,"tmax",type,"sdfopen",False))
    wind = np.squeeze(Extract_Data_Period_Average(idate_tstep,fdate_tstep,dt_down,dt_up,dt,forcing_file,"wind",type,"sdfopen",False))
  #Otherwise use the GFS analysis fields as forcings (TRMM period)
  else: 
    vic_file = 'http://stream.princeton.edu:9090/dods/LAFDM/VIC_3B42RT/DAILY'
    forcing_file = 'http://stream.princeton.edu:9090/dods/LAFDM/GFS_ANALYSIS_BC/DAILY'
    r_net = np.squeeze(Extract_Data_Period_Average(idate_tstep,fdate_tstep,dt_down,dt_up,dt,vic_file,"r_net",type,"sdfopen",False))
    tmin = np.squeeze(Extract_Data_Period_Average(idate_tstep,fdate_tstep,dt_down,dt_up,dt,forcing_file,"tmin",type,"sdfopen",False))
    tmax = np.squeeze(Extract_Data_Period_Average(idate_tstep,fdate_tstep,dt_down,dt_up,dt,forcing_file,"tmax",type,"sdfopen",False))
    wind = np.squeeze(Extract_Data_Period_Average(idate_tstep,fdate_tstep,dt_down,dt_up,dt,forcing_file,"wind",type,"sdfopen",False))
  
  #Daily avg temp (C)
  tavg = (tmax+tmin)/2 - 273.15
  #Sat  and actual vap pressure (kPa)
  es = 0.6108*np.exp(np.divide(17.27*tavg,237.3+(tavg)))
  ea = 0.6108*np.exp(np.divide(17.27*(tmin-273.15),237.3+(tmin-273.15)))
  #VPD 
  D = es - ea
  #Delta (kPa/C)
  delta = 4098*np.divide(es,np.power(237.3+tavg,2))
  #Psychrometric Const
  z1=20; #Set the elevation above the sea level as 20m
  P=101.3*((293-0.0065*z1)/293)**5.26;
  g=0.0010013*P/(0.622*2.45);
  
  Eto1 = np.multiply(np.divide(delta,delta+g*(1+0.33*wind)),r_net*0.0864)
  Eto2 = np.multiply(np.multiply(np.divide(delta,delta+g*(1+0.33*wind)),np.divide(900,tavg+275)),np.multiply(D,wind))
  Eto = np.squeeze(Eto1 + Eto2)
  Eto[Eto<0] = 0
  
  plt.figure()
  plt.imshow(np.flipud(Eto*mask))
  plt.colorbar()
  plt.savefig('imgs/ETo_%s.png' % date.strftime('%Y%m%d'))
  plt.close()

  return

## Start of Main Code
#Specify region dimensions
#Should be the same as ctl files you are pulling from
dims = {}
dims['minlat'] = -55.875000 
dims['minlon'] = -118.375000 
dims['nlat'] = 357 
dims['nlon'] = 357
dims['res'] = 0.250
dims['maxlat'] = dims['minlat'] + dims['res']*(dims['nlat']-1)
dims['maxlon'] = dims['minlon'] + dims['res']*(dims['nlon']-1)
dt = datetime.timedelta(days=1)
#fdate = datetime.datetime.today() - 3*dt
fdate = datetime.datetime(2002,1,31)
idate = datetime.datetime(2002,1,1)

#Extra date variables
date = idate
n = (fdate - idate).days
#print n

#Initialize multiproccessing job 
num = 0
p = []
#Specify number of threads to use
pool = mp.Pool(processes=16)
#Call function with a set of args (needs to come from a list or for loop that can be iterated)
results = [pool.apply_async(Calc_ETo,args=([date + datetime.timedelta(days=num)])) for num in range(0,n+1)]
#Collect all output from function
#None here since we just save images
output = [p.get() for p in results]

