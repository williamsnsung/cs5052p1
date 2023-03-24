from pyspark.sql.functions import *
from operator import add
from functools import reduce
import heapq
import plotly.express as px

def getLas(data):
    """Gets the local authorities from the dataset

    Args:
        data (dataframe): the dataset

    Returns:
        dataframe: the local authorities sorted in order and unique
    """
    return data.select("la_name").where(data.geographic_level=="Local authority").distinct().sort("la_name")

def getLaEnrlmnts(data, la):
    """Gets the enrolments for the given local authority

    Args:
        data (dataframe): the dataset
        la (string): the local authority to get enrolments for

    Returns:
        dataframe: the enrolments for that local authority broken down by year
    """
    res = data.select("enrolments", "time_period").orderBy("time_period")\
        .filter((data.la_name.isin(la)) & (data.geographic_level=="Local authority") & (data.school_type == 'Total'))
    return res.withColumn("enrolments", res["enrolments"].cast('int'))

def getSchls(data):
    """Gets the schools from the dataset

    Args:
        data (dataframe): the dataset

    Returns:
        dataframe: the schools unique and sorted
    """
    return data.select("school_type").where((data.school_type != 'Total') & (data.geographic_level=="School")).distinct().sort("school_type")

def getSchlMedAbs(data, schl):
    """Gets school medical absences for the given school type as a sum of appointments and illness absences

    Args:
        data (dataframe): the dataset
        schl (string): the school type

    Returns:
        dataframe: dataframe with only the total as a single col and row
    """
    res = data.select("sess_auth_appointments", "sess_auth_illness")\
        .filter((data.time_period == "2017/18") & (data.school_type.isin(schl) & (data.geographic_level=="National")))\
        .agg({"sess_auth_appointments" : "sum", "sess_auth_illness" : "sum"})
    return res.withColumn('total', reduce(add, [col(x) for x in res.columns]))

def getYears(data):
    """Gets the time periods from the data set

    Args:
        data (data): the dataset

    Returns:
        dataframe_: years dataframe unique and sorted
    """
    return data.select("time_period").where(data.geographic_level=="National").distinct().sort("time_period")

def getUnauthAbs(data, year, opt):
    """Gets unauthorised absences broken down by the option specified on the given year

    Args:
        data (data): the dataset
        year (string): the year to match
        opt (string): the selected option to break down the data

    Returns:
        dataframe: the unauthorised absences broken down by total number of unauthorised sessions
    """
    optMap = {"Local authority" : "la_name", "Regional" : "region_name"}
    res = data.select(optMap[opt], "sess_unauthorised").filter(data.time_period.isin(year) & (data.geographic_level==opt) & (data.school_type == 'Total')).sort(optMap[opt])
    return res.withColumn("sess_unauthorised", res["sess_unauthorised"].cast('int'))

def getTop3Auth(data):
    """Gets the top 3 reasons for absences each year

    Args:
        data (data): the dataset

    Returns:
        dictionary: the top 3 reasons for absences for each year
    """
    authList = ["sess_auth_appointments", "sess_auth_excluded", "sess_auth_ext_holiday", "sess_auth_holiday", "sess_auth_illness",\
         "sess_auth_other", "sess_auth_religious", "sess_auth_study", "sess_auth_traveller"]
    authDict = dict((i, "sum") for i in authList)
    res = data.select(authList + ["time_period"]).sort("time_period").groupBy("time_period").agg(authDict)
    for col in res.schema.names:
        if col[:3] == "sum":
            res = res.withColumnRenamed(col, col[4:-1])

    collect = res.collect()
    ranking = {}
    # heap sort and pop top 3 to get top 3
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
    """Gets the required data for comparison as specified

    Args:
        data (data): the dataset
        cols (list): list of strings of columns to select
        sumMap (dictionary): the columns to aggregate
        la (string): local authority
        year (string): time period
        groupBy (string): column to group by

    Returns:
        dataframe: desired data as specified
    """
    res = data.select(cols)\
        .where((data.la_name == la) & (data.time_period == year) & (data.geographic_level=="Local authority") & (data.school_type != 'Total'))\
        .groupBy(groupBy).agg(sumMap)

    for col in res.schema.names:
        if col[:3] == "sum":
            res = res.withColumnRenamed(col, col[4:-1])
    return res

def getPercentages(data, numer, denom):
    """returns a dataframe with a new column added representing a columns percentage based on the given denominator

    Args:
        data (data): the dataset
        numer (string): numerator column
        denom (string): denominator column

    Returns:
        _type_: _description_
    """
    return data.withColumn(f"{numer}_percent", round(data[numer] / data[denom] * 100, 3))

def getAbsLocSchl(data, year):
    """Gets dataframe for relationship between absences location and schools

    Args:
        data (data): the dataset
        year (string): time period

    Returns:
        dataframe: dataset broken down into region name overall percentage of absences and school type
    """
    return data.select("region_name", "sess_overall_percent", "school_type")\
        .filter(data.time_period.isin(year) & (data.geographic_level=="Regional") & (data.school_type != 'Total')).sort("time_period")

def flatten(data):
    """flattens a dataframe

    Args:
        data (data): the dataset

    Returns:
        list: flattened dataframe
    """
    return [j for i in data for j in i]

def getPercentageTable(data):
    """Creates a dataframe of pecentages

    Args:
        data (data): the dataset

    Returns:
        dataframe: dataframe only containing columns ending with _percent and the local authority
    """
    res = data.select([c for c in data.schema.names if c.endswith("percent") or c == "la_name"])
    for col in res.schema.names:
        if col[:10] == "sess_auth_":
            res = res.withColumnRenamed(col, col[10:])
        elif col[:4] == "sess":
            res = res.withColumnRenamed(col, col[5:])
    return res

def getAnalysis(data):
    """Gets the line chart, heat map of regions ranked by absences from best to worst over time, and the average ranking of regions over time

    Args:
        data (data): the dataset

    Returns:
        figure, figure, dictionary: the line chart, heatmap, and average rankings
    """
    res = data.select("time_period", "sess_overall_percent", "region_name")\
        .filter((data.geographic_level=="Regional") & (data.school_type == 'Total'))\
        .sort("time_period")
    nat = data.select("time_period", "sess_overall_percent")\
        .filter((data.geographic_level=="National") & (data.school_type == 'Total'))\
        .sort("time_period")
    nat = nat.withColumn("region_name", lit("Nation Wide"))
    nat = nat.union(res)
    nat.show()
    fig1 = px.line([row.asDict() for row in nat.collect()], x="time_period", y="sess_overall_percent", color='region_name')
    fig1.update_layout(autotypenumbers='convert types')

    labels = dict(per="time_period", locs="region_name", color="rank")
    pers = flatten(getYears(data).collect())
    locs = flatten(res.select("region_name").distinct().collect())

    res = res.collect()
    for i in range(len(res)):
        res[i] = res[i].asDict()

    # create an intermiedary matrix to translate the data into the correct format for the heat map
    intMatrix = dict(zip(locs, [dict() for i in range(len(locs))]))
    for loc in intMatrix:
        intMatrix[loc] = dict(zip(pers, [0 for i in range(len(pers))]))

    for i in range(len(res)):
        per = res[i]["time_period"]
        loc = res[i]["region_name"]
        val = res[i]["sess_overall_percent"]
        intMatrix[loc][per] = val

    # convert the intermediary matrix into a normal 2d array for input into the heat map
    matrix = [[0] * len(pers) for i in range(len(locs))]
    i = 0
    for loc in intMatrix:
        j = 0
        for per in intMatrix[loc]:
            matrix[i][j] = intMatrix[loc][per]
            j += 1
        i += 1

    # update the matrix to represent the ranks of each region each year
    for per in range(len(matrix[0])):
        rankings = []
        for loc in range(len(matrix)):
            heapq.heappush(rankings, (matrix[loc][per], loc))
        for pos in range(1, len(matrix) + 1):
            _, loc = heapq.heappop(rankings)
            matrix[loc][per] = pos
    fig2 = px.imshow(matrix, labels = labels, x = pers, y = locs)

    # find the average ranking of each region using heap sort
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