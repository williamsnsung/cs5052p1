from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from sparkcmds import *
import inquirer

# TODO take csv as user input

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

features = {"Core Features":coreF, "Intermediate Features":intF}

laList = flatten(getLas(data).collect())
timePer = flatten(getYears(data).collect())
schType = flatten(getSchls(data).collect())

loop = True
while loop:

    fGroup = [
        inquirer.List('fGroup', message='Select a Feature Group', choices=("Core Features", "Intermediate Features"), carousel=True)
    ]

    fGroup = inquirer.prompt(fGroup)

    feature = [
        inquirer.List('feature', message='Select a Feature to View', choices=features[fGroup["fGroup"]], carousel=True)
    ]

    feature = inquirer.prompt(feature)['feature']

    if feature == pupEn:
        print("Pupil Enrolements in Local Authorities")
        laNames = [inquirer.Checkbox("laNames", message='Select Your Local Authorities', choices=laList, default = laList[:1], carousel=True)]
        laNames = inquirer.prompt(laNames)["laNames"]
        for laName in laNames:
            res = getLaEnrlmnts(data, laName)
            res = res.withColumn("enrolments", format_number(res["enrolments"], 0))
            print(f"Pupil Enrolements in {laName}")
            res.show(res.count(), False)
            print()
    elif feature == schAuth:
        print("Authorised Medical Absences Between 2017-2018")
        sch = [inquirer.List('sch', message='Select a School Type', choices=(schType), carousel=True)]
        sch = inquirer.prompt(sch)['sch']
        res = getSchlMedAbs(data, sch)
        print("The total number of pupils who were given an authorised absence because of a medical appointment or illness in 2017-2018 at a",
            sch, 
            "school was",
            f"{int(res.collect()[0].asDict()['total']):,}"
        )
    elif feature == unauth:
        print("Unauthorised absences broken down by either region name or local authority name")
        per = [inquirer.List('per', message='Select a Time Period', choices=timePer, carousel=True)]
        per = inquirer.prompt(per)['per']
        opt = [inquirer.List('opt', message='Select How to Break Down The Data', choices=("Local authority", "Regional"), carousel=True)]
        opt = inquirer.prompt(opt)['opt']
        res = getUnauthAbs(data, per, opt)
        res = res.withColumn("unauthorised", format_number(res["unauthorised"], 0))
        res.show(res.count(), False)
    elif feature == auth:
        print("Top 3 Reasons for authorised absences in each year:\n")
        res = getTop3Auth(data)
        res = spark.createDataFrame(data = res, schema=["time_period", "1st", "2nd", "3rd"])
        res.show(res.count(), False)
    elif feature == cmpAuth:
        print("Comparison of Two Local Authorities in a Given year")
        per = [inquirer.List('per', message='Select a Time Period', choices=timePer, carousel=True)]
        per = inquirer.prompt(per)['per']

        la1 = [inquirer.List("la1", message='Select Your First Local Authority', choices=laList, carousel=True)]
        la1 = inquirer.prompt(la1)["la1"]
        la2 = [inquirer.List("la2", message='Select Your Second Local Authority', choices=laList, carousel=True)]
        la2 = inquirer.prompt(la2)["la2"]
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

        print("Enrolements")
        print(enrl[0]["la_name"], f"{int(enrl[0]['enrolments']):,}")
        print(enrl[1]["la_name"], f"{int(enrl[1]['enrolments']):,}")
        print()

        print("Percentage of Persistent Absentees")
        print(enrl[0]["la_name"], str(enrl[0]["enrolments_pa_10_exact_percent"]) + "%")
        print(enrl[1]["la_name"], str(enrl[1]["enrolments_pa_10_exact_percent"]) + "%")
        print()

        schl1 = res["schl"][0]
        schl1 = schl1.withColumn('total', lit(schl1.select(sum(schl1.num_schools)).collect()[0][0]))
        schl1 = getPercentages(schl1, "num_schools", "total")
        schl1 = schl1.select("school_type", "num_schools_percent")

        schl2 = res["schl"][1]
        schl2 = schl2.withColumn('total', lit(schl2.select(sum(schl2.num_schools)).collect()[0][0]))
        schl2 = getPercentages(schl2, "num_schools", "total")
        schl2 = schl2.select("school_type", "num_schools_percent")

        print("School Type Distribution")
        print(la[0])
        schl1.show(schl1.count(), False)
        print(f"\n{la[1]}")
        schl2.show(schl2.count(), False)
        print()

        print("Number of Overall Sessions")
        sess = res["sess"][0].union(res["sess"][1])
        sessTable = sess.collect()
        for i in range(len(sessTable)):
            sessTable[i] = sessTable[i].asDict()
        print(sessTable[0]["la_name"], f"{int(sessTable[0]['sess_possible']):,}")
        print(sessTable[1]["la_name"], f"{int(sessTable[1]['sess_possible']):,}")
        print()

        print("Percentage of Authorised vs Unauthorised Sessions")
        sess = sess.withColumn("sess_normal", sess["sess_possible"] - sess["sess_overall"] - sess["sess_overall_pa_10_exact"])
        sess = getPercentages(sess, "sess_normal", "sess_possible")
        sess = getPercentages(sess, "sess_overall", "sess_possible")
        sess = getPercentages(sess, "sess_overall_pa_10_exact", "sess_possible")
        sess = getPercentageTable(sess)
        sess.show(sess.count(), False)
        print()

        print("Percentages of Authorised Absences")
        auth = res["auth"][0].union(res["auth"][1])
        for col in auth.schema.names:
            if col == "sess_auth_totalreasons" or col == "la_name":
                continue
            auth = getPercentages(auth, col, "sess_auth_totalreasons")
        auth = getPercentageTable(auth)
        auth.show(auth.count(), False)
        print()

        print("Percentages of Unauthorised Absences")
        unauth = res["unauth"][0].union(res["unauth"][1])
        for col in unauth.schema.names:
            if col == "sess_unauth_totalreasons" or col == "la_name":
                continue
            unauth = getPercentages(unauth, col, "sess_unauth_totalreasons")
        unauth = getPercentageTable(unauth)
        unauth.show(unauth.count(), False)

    elif feature == perfReg:
        print("Performance of Regions in England from 2006-2018")
        fig1, fig2, rankings = getAnalysis(data)
        print("Displaying Absence Rate Line Chart...")
        fig1.show()
        print("Displaying Region Ranking by Absence Rate Heat Map...")
        fig2.show()
        res = spark.createDataFrame(rankings, ["rank (best -> worst)", "region_name"])
        print("Ranking of Average Absence Rate Per Region from 2006-2018")
        res.show(res.count(), False)

    print()
    loop = [inquirer.List('loop', message='Continue?', choices=["Yes", "No"], carousel=True)]
    loop = True if inquirer.prompt(loop)['loop'] == "Yes" else False