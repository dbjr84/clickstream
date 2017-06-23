# clickstream
This repo contains Spark on Scala code to calculate times watched of a video by a user. It contains test data to run the code. Test data are collections of dummy click stream data. The data contains the length of the video, the click type, user info and offset point of the video. The code calculates distinct time and cumulative time watched by the user

# 1. Distinct time 
Distinct time is the unique length of the video watch by the user. Distinct time can not be greater than the actual lenght of the video

# 2. Cumulative time
Cumulative time is the total time user spent watching the video. It counts any repeataion of any portion of the video.

# To run
Download the code. 
 - mvn clean install

