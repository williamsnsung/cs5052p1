from pyspark.sql.functions import *
from operator import add
from functools import reduce
import heapq
import plotly.express as px

def getLas(data):
    return data.select("la_name").where(data.geographic_level=="Local authority").distinct().sort("la_name")

def getLaEnrlmnts(data, la):
    res = data.select("enrolments", "time_period").orderBy("time_period")\
        .filter((data.la_name.isin(la)) & (data.geographic_level=="Local authority") & (data.school_type == 'Total'))
    return res.withColumn("enrolments", res["enrolments"].cast('int'))

def getSchls(data):
    return data.select("school_type").where((data.school_type != 'Total') & (data.geographic_level=="School")).distinct().sort("school_type")

def getSchlMedAbs(data, schl):
    res = data.select("sess_auth_appointments", "sess_auth_illness")\
        .filter((data.time_period == "2017/18") & (data.school_type.isin(schl) & (data.geographic_level=="National")))\
        .agg({"sess_auth_appointments" : "sum", "sess_auth_illness" : "sum"})
    return res.withColumn('total', reduce(add, [col(x) for x in res.columns]))

def getYears(data):
    return data.select("time_period").where(data.geographic_level=="National").distinct().sort("time_period")

def getUnauthAbs(data, year, opt):
    optMap = {"Local authority" : "la_name", "Regional" : "region_name"}
    res = data.select(optMap[opt], "sess_unauthorised").filter(data.time_period.isin(year) & (data.geographic_level==opt) & (data.school_type == 'Total')).sort(optMap[opt])
    return res.withColumn("sess_unauthorised", res["sess_unauthorised"].cast('int'))

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

def getAttendance200618(data):
    return data.select("time_period", "region_name", "sess_overall_percent")\
        .filter((data.time_period != "2018-19") & (data.geographic_level=="Regional") & (data.school_type == 'Total'))\
        .sort("time_period")

def getAbsLocSchl(data, year):
    return data.select("region_name", "sess_authorised_percent", "school_type")\
        .filter(data.time_period.isin(year) & (data.geographic_level=="Regional") & (data.school_type != 'Total')).sort("time_period")

def flatten(data):
    return [j for i in data for j in i]

def getPercentageTable(data):
    res = data.select([c for c in data.schema.names if c.endswith("percent") or c == "la_name"])
    for col in res.schema.names:
        if col[:10] == "sess_auth_":
            res = res.withColumnRenamed(col, col[10:])
        elif col[:4] == "sess":
            res = res.withColumnRenamed(col, col[5:])
    return res

def getAnalysis(data):
    res = getAttendance200618(data)
    fig1 = px.line([row.asDict() for row in res.collect()], x="time_period", y="sess_overall_percent", color='region_name')
    fig1.update_layout(autotypenumbers='convert types')

    labels = dict(per="time_period", locs="region_name", color="rank")
    pers = flatten(getYears(data).collect())
    locs = flatten(res.select("region_name").distinct().collect())

    res = res.collect()
    for i in range(len(res)):
        res[i] = res[i].asDict()

    intMatrix = dict(zip(locs, [dict() for i in range(len(locs))]))
    for loc in intMatrix:
        intMatrix[loc] = dict(zip(pers, [0 for i in range(len(pers))]))

    for i in range(len(res)):
        per = res[i]["time_period"]
        loc = res[i]["region_name"]
        val = res[i]["sess_overall_percent"]
        intMatrix[loc][per] = val

    matrix = [[0] * len(pers) for i in range(len(locs))]
    i = 0
    for loc in intMatrix:
        j = 0
        for per in intMatrix[loc]:
            matrix[i][j] = intMatrix[loc][per]
            j += 1
        i += 1

    for per in range(len(matrix[0])):
        rankings = []
        for loc in range(len(matrix)):
            heapq.heappush(rankings, (matrix[loc][per], loc))
        for pos in range(1, len(matrix) + 1):
            _, loc = heapq.heappop(rankings)
            matrix[loc][per] = pos
    fig2 = px.imshow(matrix, labels = labels, x = pers, y = locs)

    rankHeap = []
    for loc, loci in zip(locs, [i for i in range(len(matrix))]):
        tot = 0
        for per in range(len(matrix[0])):
            tot += matrix[loci][per]
        heapq.heappush(rankHeap, (tot / len(matrix[0]), loc))
    
    rankings = []
    for i in range(1, len(rankHeap) + 1):
        pos, loc = heapq.heappop(rankHeap)
        rankings.append((i, loc))
    return fig1, fig2, rankings