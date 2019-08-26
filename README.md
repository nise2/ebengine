# Ebengine

## Table of contents

1. [Introduction](#introduction)
2. [Tools Versions](#tools-versions)
3. [Run the job](#run-job)
4. [Documentation](#documentation)
5. [Tests](#tests)
6. [Structure](#structure)

### Introduction
E.B. Engine is a job built on top of *Spark* and *Scala*.

It allows you to generate DataFrames according to certain characteristics, and store them into files.<br />

Also, you might be interested to generate the *Scaladoc* to have more information about the job in general (funcions, parameters...).

Finally, this project also contains some *unit and integrations tests*.

### Tools Versions
|  Tool | Version  |
|---|---|
|  Spark |  2.4.3 |
|  Scala |  2.12  |
|  ScalaTest |  3.0.8  |
|  Maven |  3.4.6     |

For more details, you can take a look to the properties section of the pom.xml :
```
<properties>
	...
        <commons.collections.version>3.2.2</commons.collections.version>
        <scala.version>2.12</scala.version>
        <scalatest.version>3.0.8</scalatest.version>

        <spark.core.version>2.4.3</spark.core.version>

        <maven.shade.plugin.version>3.2.1</maven.shade.plugin.version>
        <maven.surefire.plugin.version>3.0.0-M3</maven.surefire.plugin.version>
        <scalatest.maven.plugin.version>2.0.0</scalatest.maven.plugin.version>
        <scala.compat.version>2.12.</scala.compat.version>
        <scala.maven.plugin.version>3.4.6</scala.maven.plugin.version>
    </properties>
```
### Run the job
Make sure you have [Spark](https://spark.apache.org/docs/latest/index.html) installed, then you can use [*spark-submit*](https://spark.apache.org/docs/latest/submitting-applications.html) to launch the job.<br />

Feel free to accomodate the resource configuration with your usage.
```
./spark-submit --class com.dng.ebengine.Main --master local ebengine.jar 
```

### Documentation
There is a documentation of the project generated with ScalaDoc.<br />
You can generate it with the command *mvn-site*.

### Tests
Here is the list of the tested functionalities :
```
// LookupItemTest
it("should generate the expected file content")
it("should create a file in the expected path")

// LookupUserTest
it("should generate the expected file content")
it("should create a file in the expected path")

// AggRatingsTest
it("should return the max timestamp value: ..." + " given ...")
it("should return the number of days in ms given a timestamp")
it("should return the penalty factor given a rating and its gap timestamp with max")
it("should return the rating penalty given a rating and a number of days")
it("should return the rating without penalty given timestampMax - timestamp <= 0")
it("should return the rating with penalty given timestampMax - timestamp > 0")
```

### Structure
Below are listed the main directories and files.
- ./ (root directory)
```
- pom.xml
- ebengine.jar
```

- In src/main
```
- resources/input/ # Contains the input files to the program
- resources/output/ # Will be created when creating files from the Main
- com.dng.ebengine/
   -- lookup/<br/>
       -- AggRatings<br />
       -- LookupItem<br />
       -- LookupUser<br />
   -- utils/<br />
      -- EbengineConf # Contains the project constants
```
- In src/test
```
- resources/output/ # Will be created when creating files from the Tests
- com.dng.ebengine/
   -- lookup/<br/> ## Tests of the matching package
       -- AggRatingsTest<br />
       -- LookupItemTest<br />
       -- LookupUserTest<br />
   -- EbengineConf # Contains the project constants
   -- EbengineConfTestUtils # Contains the tests constants
```

To have a more general overview of the project, you can eventually take a look here:
```
dng@local:~/Projects/ebengine$ tree
.
├── ebengine-1.0-SNAPSHOT-with-dependencies.jar/
├── ebengine.jar/
├── pom.xml
├── scaladoc/
├── src
│   ├── main 
│   │   ├── resources
│   │   │   └── input
│   │   │       ├── expected_lookup_product_100.csv
│   │   │       ├── expected_lookup_user_100.csv
│   │   │       ├── xag_100.csv
│   │   │       ├── xag_101.csv
│   │   │       └── xag_102.csv
│   │   └── scala
│   │       └── com
│   │           └── dng
│   │               └── ebengine
│   │                   ├── EbengineConf.scala
│   │                   ├── Main.scala
│   │                   ├── lookup 
│   │                   │   ├── ALookup.scala
│   │                   │   ├── AggRatings.scala
│   │                   │   ├── LookupItem.scala
│   │                   │   └── LookupUser.scala
│   │                   └── utils
│   │                       ├── DataFrameUtils.scala
│   │                       └── FormatterUtils.scala
│   └── test
│       ├── resources
│       └── scala
│           └── com
│               └── dng
│                   └── ebengine
│                       ├── ContextUtils.scala
│                       ├── EbengineConfTestUtils.scala
│                       └── lookup
│                           ├── AggRatingsTest.scala
│                           ├── LookupItemTest.scala
│                           └── LookupUserTest.scala
└── target
 ```
 


