from timeit import default_timer as timer

start = timer()

file_in = open("mock_weather_data_2020-2021.txt", "r")
file_out = open("mock_weather_data_2020-2021_output.txt", "w")
k = 0
sum = 0
date = "2020-01-01"
for line in file_in:
  datetime, windspeed,winddir,humidity,temperature,pressure,uv = line.split(",")
  new_date = datetime.split(" ")[0]

  temp = float(temperature)
  sum +=temp
  k += 1.0

  if date != new_date:
    #flush daily data
    data = "{}\t{}".format(date, sum/k)
    file_out.write("{}\n".format(data))
    #print(data)

    sum, k = 0, 0


    date = new_date



file_in.close()
file_out.close()

end = timer()
print("Executed in :", end - start) # Time in seconds, e.g. 5.38091952400282


