docker compose up
![img.png](img.png)

broker
docker exec --interactive --tty broker kafka-console-consumer --bootstrap-server broker:9092 --topic R2
![img_1.png](img_1.png)


kafka consumer
docker exec --interactive --tty broker kafka-console-producer --bootstrap-server broker:9092 --topic R2
![img_2.png](img_2.png)

consumer from beginning
docker exec --interactive --tty broker kafka-console-producer --bootstrap-server broker:9092 --topic R2 --from-beginning
![img_3.png](img_3.png)

controller 
![img_4.png](img_4.png)

handlers
![img_5.png](img_5.png)
![img_6.png](img_6.png)

supplier
![img_7.png](img_7.png)

kstream
R3
![img_8.png](img_8.png)
R4
![img_9.png](img_9.png)

kafaka console consumer
![img_10.png](img_10.png)

groupbykey
![img_11.png](img_11.png)

windowedBy
![img_12.png](img_12.png)

analytics
![img_13.png](img_13.png)

visualisation
![img_14.png](img_14.png)


