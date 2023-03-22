from pyspark.sql.functions import *
from operator import add
from functools import reduce
import heapq

def getLas(data):
    return data.select("la_name").where(data.la_name != '').distinct().sort("la_name")

def getLaEnrlmnts(data, la):
    return data.select("enrolments", "time_period").orderBy("time_period")\
        .filter((data.la_name.isin(la)) & (data.geographic_level=="Local authority") & (data.school_type != 'Total'))\
        .groupBy("time_period").agg(sum("enrolments").alias("enrolments"))

def getSchls(data):
    return data.select("school_type").where((data.school_type != 'Total') & (data.geographic_level=="School")).distinct().sort("school_type")

def getSchlMedAbs(data, schl):
    res = data.select("sess_auth_appointments", "sess_auth_illness")\
        .filter((data.time_period == "2017-18") & (data.school_type.isin(schl) & (data.geographic_level=="School")))\
        .agg({"sess_auth_appointments" : "sum", "sess_auth_illness" : "sum"})
    return res.withColumn('total', reduce(add, [col(x) for x in res.columns]))

def getYears(data):
    return data.select("time_period").distinct().sort("time_period")

def getUnauthAbs(data, year, opt):
    optMap = {"Local authority" : "la_name", "Regional" : "region_name"}
    return data.select(optMap[opt], "sess_unauthorised").filter(data.time_period.isin(year) & (data.geographic_level==opt) & (data.school_type != 'Total'))\
        .groupBy(optMap[opt]).agg(sum("sess_unauthorised").alias("unauthorised")).sort(optMap[opt])

def getTop3Auth(data):
    authList = ["sess_auth_appointments", "sess_auth_excluded", "sess_auth_ext_holiday", "sess_auth_holiday", "sess_auth_illness",\
         "sess_auth_other", "sess_auth_religious", "sess_auth_study", "sess_auth_traveller"]
    authDict = dict((i, "sum") for i in authList)
    res = data.select(authList + ["time_period"]).sort("time_period").groupBy("time_period").agg(authDict)
    for col in res.schema.names:
        if col[:3] == "sum":
            res = res.withColumnRenamed(col, col[4:-1])

    collect = res.collect()
    ranking = {}
    for row in collect:
        ranking[row[0]] = []
        for i in range(1, len(row)):
            heapq.heappush(ranking[row[0]], (-row[i] if row[i] is not None else 0, res.schema.names[i][10:]))
    res = []
    for year in ranking:
        res.append([year])
        for i in range(3):
            res[-1].append(heapq.heappop(ranking[year])[1])
    
    return res

def getCmpData(data, cols, sumMap, la, year, groupBy):
    res = data.select(cols)\
        .where((data.la_name == la) & (data.time_period == year) & (data.geographic_level=="Local authority") & (data.school_type != 'Total'))\
        .groupBy(groupBy).agg(sumMap)

    for col in res.schema.names:
        if col[:3] == "sum":
            res = res.withColumnRenamed(col, col[4:-1])
    return res

def getPercentages(data, numer, denom):
    return data.withColumn(f"{numer}_percent", round(data[numer] / data[denom] * 100, 3))