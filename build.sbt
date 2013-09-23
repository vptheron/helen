//      Copyright (C) 2013 Vincent Theron
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

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.10.2"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "1.0.3"

libraryDependencies += "com.typesafe" % "scalalogging-slf4j_2.10" % "1.0.1"

libraryDependencies += "org.specs2" % "specs2_2.10" % "2.2" % "test"

libraryDependencies += "org.mockito" % "mockito-core" % "1.9.5" % "test"
