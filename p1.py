import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from operator import add
from functools import reduce
import plotly.express as px
import heapq
from sparkcmds import *
from millify import prettify

dataPath = "./data/Absence_3term201819_nat_reg_la_sch.csv"
spark = SparkSession.builder.master("local").appName("p1").config("conf-key", "conf-value").getOrCreate()
data = spark.read.format("csv").option("header", "true").load(dataPath).withColumn("time_period", regexp_replace("time_period", "(..$)", "-$1"))

pupEn = "Pupil Enrolements"
schAuth = "Authorised Medical Absences"
unauth = "Unauthorised Absences"
auth = "Authorised Absences"
coreF = (pupEn, schAuth, unauth, auth)

cmpAuth = "Compare Local Authorities"
perfReg = "Performance of Regions"

intF = (cmpAuth, perfReg)

fGroup = st.sidebar.selectbox(
    'Select a Feature Group',
    ("Core Features", "Intermediate Features", "Advanced Features")
)

features = {"Core Features":coreF, "Intermediate Features":intF}

feature = st.sidebar.selectbox(
    'Select a Feature to View',
    features[fGroup]
)
if feature == pupEn:
    st.write("Pupil Enrolements in Local Authorities")
    laList = getLas(data)
    laNames = st.multiselect('Select Your Local Authorities',(laList))
    for laName in laNames:
        res = getLaEnrlmnts(data, laName)
        st.write(f"Pupil Enrolements in {laName}")
        st.dataframe(res, use_container_width=True)
elif feature == schAuth:
    st.write("Authorised Medical Absences Between 2017-2018")
    schType = getSchls(data)
    sch = st.selectbox('Select a School Type',(schType))
    res = getSchlMedAbs(data, sch)
    st.write(f"The total number of pupils who were given authorised absences because of medical appointments or illness in the time period 2017-2018 at \
        {sch} schools was {int(res.collect()[0][0]):,}")
elif feature == unauth:
    st.write("Unauthorised absences broken down by either region name or local authority name.")
    timePer = getYears(data)
    per = st.selectbox('Select a Time Period',(timePer))
    opt = st.selectbox('Select How to Break Down The Data', ("Local authority", "Regional"))
    res = getUnauthAbs(data, per, opt)
    st.dataframe(res, height=None, use_container_width=True)
elif feature == auth:
    st.write("Top 3 Reasons for authorised absences in each year")
    res = getTop3Auth(data)
    st.dataframe(spark.createDataFrame(data = res, schema=["time_period", "1st", "2nd", "3rd"]))
elif feature == cmpAuth:
    st.write("Comparison of Two Local Authorities in a Given year")

    timePer = data.select("time_period").distinct().sort("time_period")
    per = st.selectbox('Select a Time Period',(timePer))

    laList = data.select("la_name").where(data.la_name != '').distinct().sort("la_name")
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

    sessCmp = ["sess_authorised", "sess_authorised_pa_10_exact", "sess_overall", "sess_overall_pa_10_exact", "sess_overall_totalreasons",\
        "sess_possible", "sess_possible_pa_10_exact"]
    sessCmpDict = dict((i, "sum") for i in sessCmp)
    sessCmp.insert(0,"la_name")

    unauthCmp = ["sess_unauth_holiday", "sess_unauth_late", "sess_unauth_noyet", "sess_unauth_other", "sess_unauth_totalreasons", "sess_unauthorised",\
        "sess_unauthorised_pa_10_exact"]
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
    col1.metric(enrl[0]["la_name"], prettify(enrl[0]["enrolments"]))
    col2.metric(enrl[1]["la_name"], prettify(enrl[1]["enrolments"]))
    st.write("Percentage of Persistent Absentees")
    col1, col2 = st.columns(2)
    col1.metric(enrl[0]["la_name"], str(enrl[0]["enrolments_pa_10_exact_percent"]) + "%")
    col2.metric(enrl[1]["la_name"], str(enrl[1]["enrolments_pa_10_exact_percent"]) + "%")

    schl1 = res["schl"][0]
    schl2 = res["schl"][1]
    st.write("School Type Distribution")
    fig2 = px.pie(schl1.toPandas(), values='num_schools', names='school_type', title=la[0])
    fig3 = px.pie(schl2.toPandas(), values='num_schools', names='school_type', title=la[1])
    col3 = st.plotly_chart(fig2, theme="streamlit", use_container_width=True)
    col4 = st.plotly_chart(fig3, theme="streamlit", use_container_width=True)

    st.write("Percentages of Authorised Absences")
    auth = res["auth"][0].union(res["auth"][1])
    for col in auth.schema.names:
        if col == "sess_auth_totalreasons" or col == "la_name":
            continue
        auth = getPercentages(auth, col, "sess_auth_totalreasons")
    fig4 = px.bar(auth.toPandas(), x="la_name", y=[col for col in auth.schema.names if col[-8:] == "_percent"])
    st.plotly_chart(fig4, theme="streamlit", use_container_width=True)

    st.write("Number of Overall Sessions")
    sess = res["sess"][0].union(res["sess"][1])
    sess = getPercentages(sess, "sess_possible_pa_10_exact", "sess_possible")
    sess = getPercentages(sess, "sess_authorised", "sess_possible")
    sess = sess.collect()
    for i in range(len(sess)):
        sess[i] = sess[i].asDict()
    st.write(sess)
    col1, col2 = st.columns(2)
    col1.metric(sess[0]["la_name"], prettify(sess[0]["sess_possible"]))
    col2.metric(sess[1]["la_name"], prettify(sess[1]["sess_possible"]))

    st.write("Percentage of Authorised Sessions")
    col1, col2 = st.columns(2)
    col1.metric(sess[0]["la_name"], str(sess[0]["sess_authorised_percent"]) + "%")
    col2.metric(sess[1]["la_name"], str(sess[1]["sess_authorised_percent"]) + "%")


    st.write("Percentage of Sessions Possible of Persistent Absentees")
    col1, col2 = st.columns(2)
    col1.metric(sess[0]["la_name"], str(sess[0]["sess_possible_pa_10_exact_percent"]) + "%")
    col2.metric(sess[1]["la_name"], str(sess[1]["sess_possible_pa_10_exact_percent"]) + "%")