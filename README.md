# learning-spark

##### [Run dataset operations](https://github.com/techmonad/learning-spark/blob/master/src/main/scala/com/techmonad/learn/DataSetOps.scala#L7-L93) 
```shell script
 sbt "runMain com.techmonad.learn.DataSetOps"
```

##### [Run dataframe operations](https://github.com/techmonad/learning-spark/blob/master/src/main/scala/com/techmonad/learn/DataFrameOps.scala#L6-L59) 
```shell script
 sbt "runMain com.techmonad.learn.DataFrameOps"
```


##### [Run RDD operations](https://github.com/techmonad/learning-spark/blob/master/src/main/scala/com/techmonad/learn/RDDOps.scala#L5-L71) 
```shell script
 sbt "runMain com.techmonad.learn.RDDOps"
```
#### [Run Structured Streaming operations](https://github.com/techmonad/learning-spark/blob/master/src/main/scala/com/techmonad/learn/StructuredStreamingOps.scala)
```shell script
// start natcat server and paste some texts
 nc -lk 9999

 //Now start streaming app
 sbt "runMain com.techmonad.learn.StructuredStreamingOps"
```
