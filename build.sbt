//      Copyright (C) 2014 Vincent Theron
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

name := "Helen"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.2"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.5",
  "joda-time" % "joda-time" % "2.4",
  "org.joda" % "joda-convert" % "1.5",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.4"
)
