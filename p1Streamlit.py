import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import plotly.express as px
from sparkcmds import *

dataPath = "./data/Absence_3term201819_nat_reg_la_sch.csv"
spark = SparkSession.builder.master("local").appName("p1").config("conf-key", "conf-value").getOrCreate()
data = spark.read.format("csv").option("header", "true").load(dataPath).withColumn("time_period", regexp_replace("time_period", "(..$)", "/$1"))

pupEn = "Pupil Enrolements"
schAuth = "Authorised Medical Absences"
unauth = "Unauthorised Absences"
auth = "Authorised Absences"
coreF = (pupEn, schAuth, unauth, auth)

cmpAuth = "Compare Local Authorities"
perfReg = "Performance of Regions"

intF = (cmpAuth, perfReg)

anal = "Absence-Location-School Analysis"

advF = (anal, "")

laList = flatten(getLas(data).collect())
timePer = flatten(getYears(data).collect())
schType = flatten(getSchls(data).collect())

fGroup = st.sidebar.selectbox(
    'Select a Feature Group',
    ("Core Features", "Intermediate Features", "Advanced Features")
)

features = {"Core Features":coreF, "Intermediate Features":intF, "Advanced Features":advF}

feature = st.sidebar.selectbox(
    'Select a Feature to View',
    features[fGroup]
)

if feature == pupEn:
    st.write("Pupil Enrolements in Local Authorities")
    laNames = st.multiselect('Select Your Local Authorities', laList)
    for laName in laNames:
        res = getLaEnrlmnts(data, laName)
        st.write(f"Pupil Enrolements in {laName}")
        st.dataframe(res, use_container_width=True)

elif feature == schAuth:
    st.write("Authorised Medical Absences Between 2017-2018")
    sch = st.selectbox('Select a School Type', schType)
    res = getSchlMedAbs(data, sch)
    st.write(f"The total number of pupils who were given authorised absences because of medical appointments or illness in the time period 2017-2018 at \
        {sch} schools was {int(res.collect()[0][0]):,}")

elif feature == unauth:
    st.write("Unauthorised absences broken down by either region name or local authority name.")
    per = st.select_slider('Select a Time Period', options=timePer)
    opt = st.select_slider('Select How to Break Down The Data', ("Local authority", "Regional"))
    res = getUnauthAbs(data, per, opt)
    st.dataframe(res, height=None, use_container_width=True)

elif feature == auth:
    st.write("Top 3 Reasons for authorised absences in each year")
    res = getTop3Auth(data)
    st.dataframe(spark.createDataFrame(data = res, schema=["time_period", "1st", "2nd", "3rd"]))

elif feature == cmpAuth:
    st.write("Comparison of Two Local Authorities in a Given year")

    per = st.select_slider('Select a Time Period', options=timePer)

    la1 = st.selectbox('Select Your First Local Authority',(laList))
    la2 = st.selectbox('Select Your Second Local Authority',(laList))
    la = [la1, la2]

    enrlCmp = ["enrolments", "enrolments_pa_10_exact"]
    enrlCmpDict = dict((i, "sum") for i in enrlCmp)
    enrlCmp.insert(0,"la_name")

    schlCmp = ["num_schools", "school_type"]
    schlCmpDict = {"num_schools":"sum"}

    authCmp = ["sess_auth_appointments", "sess_auth_excluded", "sess_auth_ext_holiday", "sess_auth_holiday", "sess_auth_illness",\
         "sess_auth_other", "sess_auth_religious", "sess_auth_study", "sess_auth_traveller", "sess_auth_totalreasons"]
    authCmpDict = dict((i, "sum") for i in authCmp)
    authCmp.insert(0,"la_name")

    sessCmp = ["sess_possible", "sess_overall", "sess_overall_pa_10_exact"]
    sessCmpDict = dict((i, "sum") for i in sessCmp)
    sessCmp.insert(0,"la_name")

    unauthCmp = ["sess_unauth_holiday", "sess_unauth_late", "sess_unauth_noyet", "sess_unauth_other", "sess_unauth_totalreasons"]
    unauthCmpDict = dict((i, "sum") for i in unauthCmp)
    unauthCmp.insert(0,"la_name")

    res = {"enrl":[], "schl":[], "auth":[], "sess":[], "unauth":[]}

    for i in range(2):
        enrl = getCmpData(data, enrlCmp, enrlCmpDict, la[i], per, "la_name")
        schl = getCmpData(data, schlCmp, schlCmpDict, la[i], per, "school_type")
        auth = getCmpData(data, authCmp, authCmpDict, la[i], per, "la_name")
        sess = getCmpData(data, sessCmp, sessCmpDict, la[i], per, "la_name")
        unauth = getCmpData(data, unauthCmp, unauthCmpDict, la[i], per, "la_name")
        res["enrl"].append(enrl)
        res["schl"].append(schl)
        res['auth'].append(auth)
        res["sess"].append(sess)
        res["unauth"].append(unauth)
    
    enrl = res["enrl"][0].union(res["enrl"][1])
    enrl = getPercentages(enrl, "enrolments_pa_10_exact", "enrolments")
    enrl = enrl.collect()
    for i in range(len(enrl)):
        enrl[i] = enrl[i].asDict()

    st.write("Enrolements")
    col1, col2 = st.columns(2)
    col1.metric(enrl[0]["la_name"], f"{int(enrl[0]['enrolments']):,}")
    col2.metric(enrl[1]["la_name"], f"{int(enrl[1]['enrolments']):,}")
    st.write("Percentage of Persistent Absentees")
    col1, col2 = st.columns(2)
    col1.metric(enrl[0]["la_name"], f"{enrl[0]['enrolments_pa_10_exact_percent']}%")
    col2.metric(enrl[1]["la_name"], f"{enrl[1]['enrolments_pa_10_exact_percent']}%")

    schl1 = res["schl"][0]
    schl2 = res["schl"][1]
    st.write("School Type Distribution")
    fig1 = px.pie([row.asDict() for row in schl1.collect()], values='num_schools', names='school_type', title=la[0])
    fig2 = px.pie([row.asDict() for row in schl2.collect()], values='num_schools', names='school_type', title=la[1])
    col3 = st.plotly_chart(fig1, theme=None, use_container_width=True)
    col4 = st.plotly_chart(fig2, theme=None, use_container_width=True)

    st.write("Number of Overall Sessions")
    sess = res["sess"][0].union(res["sess"][1])
    sessTable = sess.collect()
    for i in range(len(sessTable)):
        sessTable[i] = sessTable[i].asDict()
    col1, col2 = st.columns(2)
    col1.metric(sessTable[0]["la_name"], f"{int(sessTable[0]['sess_possible']):,}")
    col2.metric(sessTable[1]["la_name"], f"{int(sessTable[1]['sess_possible']):,}")
    
    st.write("Percentage of Authorised vs Unauthorised Sessions")
    sess = sess.withColumn("sess_normal", sess["sess_possible"] - sess["sess_overall"] - sess["sess_overall_pa_10_exact"])
    sess = getPercentages(sess, "sess_normal", "sess_possible")
    sess = getPercentages(sess, "sess_overall", "sess_possible")
    sess = getPercentages(sess, "sess_overall_pa_10_exact", "sess_possible")
    fig1 = px.bar([row.asDict() for row in sess.collect()], x="la_name", y=[col for col in sess.schema.names if col[-8:] == "_percent"])
    st.plotly_chart(fig1, theme=None, use_container_width=True)

    st.write("Percentages of Authorised Absences")
    auth = res["auth"][0].union(res["auth"][1])
    for col in auth.schema.names:
        if col == "sess_auth_totalreasons" or col == "la_name":
            continue
        auth = getPercentages(auth, col, "sess_auth_totalreasons")
    fig1 = px.bar([row.asDict() for row in auth.collect()], x="la_name", y=[col for col in auth.schema.names if col[-8:] == "_percent"])
    st.plotly_chart(fig1, theme=None, use_container_width=True)

    st.write("Percentages of Unauthorised Absences")
    unauth = res["unauth"][0].union(res["unauth"][1])
    for col in unauth.schema.names:
        if col == "sess_unauth_totalreasons" or col == "la_name":
            continue
        unauth = getPercentages(unauth, col, "sess_unauth_totalreasons")
    fig1 = px.bar([row.asDict() for row in unauth.collect()], x="la_name", y=[col for col in unauth.schema.names if col[-8:] == "_percent"])
    st.plotly_chart(fig1, theme=None, use_container_width=True)

elif feature == perfReg:
    st.write("Performance of Regions in England from 2006-2018")
    res = getAttendance200618(data)

    fig1, fig2, rankings = getAnalysis(data)
    st.write("Absence Rate Line Chart")
    st.plotly_chart(fig1, theme=None, use_container_width=True)
    st.write("Region Ranking Heat Map")
    st.plotly_chart(fig2, theme=None, use_container_width=True)
    res = spark.createDataFrame(rankings, ["rank (best -> worst)", "region_name"])
    st.write("Ranking of Average Absence Rate Per Region from 2006-2018")
    st.dataframe(res)

elif feature == anal:
    st.write("Absence-Location-School Analysis")
    per = st.select_slider('Select a Time Period', options=timePer)
    res = getAbsLocSchl(data, per)
    labels = dict(schls="school_type", locs="region_name", color="sess_authorised_percent")
    locs = flatten(res.select("region_name").distinct().collect())
    res = res.collect()
    for i in range(len(res)):
        res[i] = res[i].asDict()

    intMatrix = dict(zip(schType, [dict() for i in range(len(schType))]))
    for schl in intMatrix:
        intMatrix[schl] = dict(zip(locs, [0 for i in range(len(locs))]))

    for i in range(len(res)):
        schl = res[i]["school_type"]
        loc = res[i]["region_name"]
        val = res[i]["sess_authorised_percent"]
        intMatrix[schl][loc] = val

    matrix = [[0] * len(locs) for i in range(len(schType))]
    i = 0
    for schl in intMatrix:
        j = 0
        for loc in intMatrix[schl]:
            matrix[i][j] = intMatrix[schl][loc]
            j += 1
        i += 1
    fig = px.imshow(matrix, labels = labels, x = locs, y = schType)
    st.plotly_chart(fig, theme=None, use_container_width=True)