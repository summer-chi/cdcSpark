#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from tempfile import mkdtemp

from sedona.spark import *
from sedona.utils.decorators import classproperty


class TestBase:

    @classproperty
    def spark(self):
        if not hasattr(self, "__spark"):
            spark = SedonaContext.create(
                SedonaContext.builder().master("local[*]").getOrCreate()
            )
            spark.sparkContext.setCheckpointDir(mkdtemp())

            setattr(self, "__spark", spark)
        return getattr(self, "__spark")

    @classproperty
    def sc(self):
        if not hasattr(self, "__spark"):
            setattr(self, "__sc", self.spark._sc)
        return getattr(self, "__sc")
