# redisUploader
Short applcation for concurrent upload files from disk to Redis. 
Accept following options:

  -path - path to directory with blob files to upload
  
  -server - Redis server to connect, i.e.: [2001:db8:f:ffff:0:0:0:1]:6139
  
  -db - Database to connect, by default - 0.
  
  -concurent Amount of concurent writes


Originally created to test Redis upload speed, but show that Redis is a damn fast database. Also, used to test Dynomite(https://github.com/Netflix/dynomite) and found that Dynomite have limitation on read ~20Kb value of key.

