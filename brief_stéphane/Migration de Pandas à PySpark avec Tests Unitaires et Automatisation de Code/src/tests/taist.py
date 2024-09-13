from __future__ import annotations

import os

import pytest
from pandaseh.aggdepartments import agg
from pandaseh.concatmedicalinfo import concat
from pandaseh.conditionalcalculationsmedicalage import cond
from pandaseh.datafilteringpatientrecords import data
from pandaseh.missingvalueshandling import miss
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from pysparkeh.aggdepartmentsspark import aggo
from pysparkeh.concatmedicalinfospark import concato
from pysparkeh.conditionalcalculationsmedicalagespark import condo
from pysparkeh.datafilteringpatientrecordsspark import datao
from pysparkeh.missingvalueshandlingspark import misso

os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

# def test_agg():
#     assertDataFrameEqual(agg(), aggo().toPandas())


def test_concat():
    assertDataFrameEqual(concat(), concato().toPandas())


def test_cond():
    assertDataFrameEqual(cond(), condo().toPandas())


def test_data():
    assertDataFrameEqual(data(), datao().toPandas())


def test_miss():
    assertDataFrameEqual(miss(), misso().toPandas())
