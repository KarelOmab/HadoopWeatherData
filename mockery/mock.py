import random
import datetime

SAMPLES_MIN=6 
MINUTE = 60 #in seconds
SAMPLE_RATE_SEC = MINUTE / SAMPLES_MIN # 60 / 6 -> 10 samples per minute


start_date = datetime.datetime(year=2020, month=1, day=1, hour=0, minute=0, second=0)
end_date   = datetime.datetime(year=2021, month=1,  day=2, hour=0, minute=0, second=0)

def rand_next(n):
    mi, ma = n
    return random.uniform(mi, ma)

#https://toronto.weatherstats.ca/charts/wind_speed-monthly.html
windKmh = {
    1 : (1, 51),
    2 : (1, 58),
    3 : (0, 62),
    4 : (1, 54),
    5 : (0, 60),
    6 : (1, 53),
    7 : (0, 48),
    8 : (0, 40),
    9 : (1, 50),
    10 : (1, 41),
    11 : (1, 44),
    12 : (1, 67)
}

windDirs = {
    1 : (0, 360),
    2 : (0, 360),
    3 : (0, 360),
    4 : (0, 360),
    5 : (0, 360),
    6 : (0, 360),
    7 : (0, 360),
    8 : (0, 360),
    9 : (0, 360),
    10 : (0, 360),
    11 : (0, 360),
    12 : (0, 360)
}

#https://toronto.weatherstats.ca/charts/relative_humidity-monthly.html
humidities = {
    1 : (30, 100),
    2 : (39, 100),
    3 : (24, 100),
    4 : (17, 100),
    5 : (20, 100),
    6 : (16, 100),
    7 : (23, 100),
    8 : (25, 100),
    9 : (35, 100),
    10 : (28, 100),
    11 : (36, 100),
    12 : (38, 100)
}



#https://toronto.weatherstats.ca/charts/temperature-monthly.html
temperatures = {
    1 : (-21.2, 4.5),
    2 : (-16.6, 10.7),
    3 : (-11.4, 18.2),
    4 : (-3.8, 19.0),
    5 : (3.7, 32.8),
    6 : (7.2, 35.5),
    7 : (13.1, 34.7),
    8 : (12.9, 33.9),
    9 : (5, 29.7),
    10 : (0.1, 23.4),
    11 : (-8.1, 19.1),
    12 : (-7.8, 17.9)
}


#https://toronto.weatherstats.ca/charts/pressure_station-monthly.html
pressureKpa = {
    1 : (96.99, 101.74),
    2 : (97.98, 101.49),
    3 : (96.93, 101.06),
    4 : (97.94, 101.29),
    5 : (98.21, 100.74),
    6 : (98.42, 100.27),
    7 : (97.79, 100.21),
    8 : (98.53, 100.17),
    9 : (97.86, 101.26),
    10 : (97.66, 101.39),
    11 : (98.26, 100.99),
    12 : (96.50, 101.45)
}

#https://toronto.weatherstats.ca/charts/forecast_uv-monthly.html
uvs = {
    1 : (2, 2),
    2 : (4, 4),
    3 : (6, 6),
    4 : (8, 8),
    5 : (10, 10),
    6 : (11, 11),
    7 : (10, 10),
    8 : (9, 9),
    9 : (7, 7),
    10 : (6, 6),
    11 : (2, 2),
    12 : (2, 2)
}

keys = ["wind", "windDir", "humidity", "temperature", "pressure", "uv"]
#initialize starting values
def init_day(datetime):
    d = {
        keys[0]: rand_next(windKmh[datetime.month]),
        keys[1]: rand_next(windDirs[datetime.month]),
        keys[2]: rand_next(humidities[datetime.month]),
        keys[3]: rand_next(temperatures[datetime.month]),
        keys[4]: rand_next(pressureKpa[datetime.month]),
        keys[5]: rand_next(uvs[datetime.month])
    }
    return d

def variance():
    return random.uniform(-0.1, 0.1)

#driver code
current_date = start_date
dt = current_date.strftime("%Y-%m-%d %H:%M:%S")
day = dt.split()[0]
di = init_day(current_date)
n = 0   #sanity check counter

f = open("temp.txt", "w")
while current_date < end_date:

    #manipulate values for each parameter?
    for k, v in di.items():
        if random.randint(0, 2) == 1:
            new_val = di[k] + variance()
            if k == keys[0]:
                di[k] = max(0, new_val) #wind speed cannot be negative
            elif k == keys[1]:
                di[k] = abs(new_val)    #wind direction cannot be negative
            elif k == keys[5]:
                di[k] = abs(min(new_val, 16)) #uv is in range of (0-16)
            else: 
                di[k] = new_val
        

    #start building output string
    dt = current_date.strftime("%Y-%m-%d %H:%M:%S")
    new_day = dt.split()[0]
    windMph = di[keys[0]]
    windDir = di[keys[1]]
    humid = di[keys[2]]
    temp = di[keys[3]]
    press = di[keys[4]]
    uv = di[keys[5]]

    d = ","  #delimiter char
    output = "{}{}{}{}{}{}{}{}{}{}{}{}{}\n".format(dt, d, windMph, d, windDir, d, humid, d, temp, d, press*10**3, d, uv)
    
    f.write(output)

    if day != new_day:
        di = init_day(current_date)
        day = new_day

    #increment loop params
    current_date += datetime.timedelta(seconds=SAMPLE_RATE_SEC)
    n += 1  #sanity check counter

f.close()
print("k", n)





