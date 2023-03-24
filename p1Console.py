from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from sparkcmds import *
import inquirer

# path to csv file, change as appropriate
dataPath = "./data/Absence_3term201819_nat_reg_la_sch.csv"
# creating the spark session
spark = SparkSession.builder.master("local").appName("p1").config("conf-key", "conf-value").getOrCreate()
# reading in the data from the csv, updating the time period column to be more human readable by adding a slash between the years
data = spark.read.format("csv").option("header", "true").load(dataPath).withColumn("time_period", regexp_replace("time_period", "(..$)", "/$1"))

# initialising list of features in each part
pupEn = "Pupil Enrolements"
schAuth = "Authorised Medical Absences"
unauth = "Unauthorised Absences"
auth = "Authorised Absences"
coreF = (pupEn, schAuth, unauth, auth)

cmpAuth = "Compare Local Authorities"
perfReg = "Performance of Regions"

intF = (cmpAuth, perfReg)

# dictionary of features used for user selection
features = {"Core Features":coreF, "Intermediate Features":intF}

# preprocessing to optimise queries later on
laList = flatten(getLas(data).collect())
timePer = flatten(getYears(data).collect())
schType = flatten(getSchls(data).collect())

# while loop for user experience, don't need to restart the program and wait for spark to begin again to try another query
loop = True
while loop:
    # prompt the user for their choice of feature group (parts)
    fGroup = [
        inquirer.List('fGroup', message='Select a Feature Group', choices=("Core Features", "Intermediate Features"), carousel=True)
    ]

    fGroup = inquirer.prompt(fGroup)

    # prompt the user for which feature from the feature group they want to select
    feature = [
        inquirer.List('feature', message='Select a Feature to View', choices=features[fGroup["fGroup"]], carousel=True)
    ]

    feature = inquirer.prompt(feature)['feature']

    # if statement to determine what query should be executed based on the selected feature
    if feature == pupEn:    # pupil enrolments per year by local authority
        print("Pupil Enrolements in Local Authorities")
        # prompts the user to select n local authorities 
        laNames = [inquirer.Checkbox("laNames", message='Select Your Local Authorities', choices=laList, default = laList[:1], carousel=True)]
        laNames = inquirer.prompt(laNames)["laNames"]
        # for each local authority, get the pupil enrolments over time for that authority and display it
        for laName in laNames:
            res = getLaEnrlmnts(data, laName)
            res = res.withColumn("enrolments", format_number(res["enrolments"], 0))
            print(f"Pupil Enrolements in {laName}")
            res.show(res.count(), False)
            print()
    elif feature == schAuth:    # authorised medical absences in 2017/18 by school type
        print("Authorised Medical Absences Between 2017-2018")
        # prompt the user to select a school type
        sch = [inquirer.List('sch', message='Select a School Type', choices=(schType), carousel=True)]
        sch = inquirer.prompt(sch)['sch']
        # retrieve the school total medical absence data and then print it
        res = getSchlMedAbs(data, sch)
        print("The total number of pupils who were given an authorised absence because of a medical appointment or illness in 2017-2018 at a",
            sch, 
            "school was",
            f"{int(res.collect()[0].asDict()['total']):,}"
        )
    elif feature == unauth:     # unauthorised absences broken down as specified for a specific year
        print("Unauthorised absences broken down by either region name or local authority name")
        # prompt the user to select a time period and how they would like to break down their data
        per = [inquirer.List('per', message='Select a Time Period', choices=timePer, carousel=True)]
        per = inquirer.prompt(per)['per']
        opt = [inquirer.List('opt', message='Select How to Break Down The Data', choices=("Local authority", "Regional"), carousel=True)]
        opt = inquirer.prompt(opt)['opt']
        # retrieve the relevant data and show the dataframe 
        res = getUnauthAbs(data, per, opt)
        res = res.withColumn("unauthorised", format_number(res["unauthorised"], 0))
        res.show(res.count(), False)
    elif feature == auth:       # top 3 reasons for authorised absence per year
        print("Top 3 Reasons for authorised absences in each year:\n")
        # retrieve data as a dictionary, create an appropriate spark dataframe, then show it
        res = getTop3Auth(data)
        res = spark.createDataFrame(data = res, schema=["time_period", "1st", "2nd", "3rd"])
        res.show(res.count(), False)
    elif feature == cmpAuth:    # compare 2 local authorities
        print("Comparison of Two Local Authorities in a Given year")
        # prompt for a year and 2 local authorities to compare
        per = [inquirer.List('per', message='Select a Time Period', choices=timePer, carousel=True)]
        per = inquirer.prompt(per)['per']

        la1 = [inquirer.List("la1", message='Select Your First Local Authority', choices=laList, carousel=True)]
        la1 = inquirer.prompt(la1)["la1"]
        la2 = [inquirer.List("la2", message='Select Your Second Local Authority', choices=laList, carousel=True)]
        la2 = inquirer.prompt(la2)["la2"]
        la = [la1, la2]

        # creating lists of features to compare along with the associated dictionaries from which an aggregate sum should be found
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

        # dictionary to store the results after passing the relevant data to the getCmpData function below
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
        
        # convert enrolements into a table so that the percentages of peristent absentees can be calculated based on the number of enrolments
        enrl = res["enrl"][0].union(res["enrl"][1])
        enrl = getPercentages(enrl, "enrolments_pa_10_exact", "enrolments")
        enrl = enrl.collect()
        for i in range(len(enrl)):
            enrl[i] = enrl[i].asDict()

        # print the enrolments for the respective local authorities
        print("Enrolements")
        print(enrl[0]["la_name"], f"{int(enrl[0]['enrolments']):,}")
        print(enrl[1]["la_name"], f"{int(enrl[1]['enrolments']):,}")
        print()

        # print the percentage of persistent absentees for the respective local authority
        print("Percentage of Persistent Absentees")
        print(enrl[0]["la_name"], str(enrl[0]["enrolments_pa_10_exact_percent"]) + "%")
        print(enrl[1]["la_name"], str(enrl[1]["enrolments_pa_10_exact_percent"]) + "%")
        print()

        # finding the total number of each school type per local authority and then calculating the percentage of the total number of schools they represent
        schl1 = res["schl"][0]
        schl1 = schl1.withColumn('total', lit(schl1.select(sum(schl1.num_schools)).collect()[0][0]))
        schl1 = getPercentages(schl1, "num_schools", "total")
        schl1 = schl1.select("school_type", "num_schools_percent")

        schl2 = res["schl"][1]
        schl2 = schl2.withColumn('total', lit(schl2.select(sum(schl2.num_schools)).collect()[0][0]))
        schl2 = getPercentages(schl2, "num_schools", "total")
        schl2 = schl2.select("school_type", "num_schools_percent")

        # displaying the resultant data frame after finding the percentages of each school type per local authority
        print("School Type Distribution")
        print(la[0])
        schl1.show(schl1.count(), False)
        print(f"\n{la[1]}")
        schl2.show(schl2.count(), False)
        print()

        # getting the number of sessions possible from the dictionary for each local authority and outputting it
        print("Number of Overall Sessions")
        sess = res["sess"][0].union(res["sess"][1])
        sessTable = sess.collect()
        for i in range(len(sessTable)):
            sessTable[i] = sessTable[i].asDict()
        print(sessTable[0]["la_name"], f"{int(sessTable[0]['sess_possible']):,}")
        print(sessTable[1]["la_name"], f"{int(sessTable[1]['sess_possible']):,}")
        print()

        print("Percentage of Normal Sessions vs Absence Sessions")
        # calculating the number of normal sessions by subtracting the overall number of absences from the number of sessions possible
        sess = sess.withColumn("sess_normal", sess["sess_possible"] - sess["sess_overall"])
        # finding the number of absence sessions that weren't by persistent absentees 
        sess = sess.withColumn("sess_overall_exc_pa", sess["sess_overall"] - sess["sess_overall_pa_10_exact"])
        # getting the percentages that normal sessions, absence sessions excluding persistent absentees and including represent
        sess = getPercentages(sess, "sess_normal", "sess_possible")
        sess = getPercentages(sess, "sess_overall_exc_pa", "sess_possible")
        sess = getPercentages(sess, "sess_overall_pa_10_exact", "sess_possible")
        sess = getPercentageTable(sess)
        sess.show(sess.count(), False)
        print()

        print("Percentages of Authorised Absences")
        # getting the percentages of each authorised reason and then creating a table of percentages from it for showing 
        auth = res["auth"][0].union(res["auth"][1])
        for col in auth.schema.names:
            if col == "sess_auth_totalreasons" or col == "la_name":
                continue
            auth = getPercentages(auth, col, "sess_auth_totalreasons")
        auth = getPercentageTable(auth)
        auth.show(auth.count(), False)
        print()
        
        # getting the percentages of each unauthorised reason and then creating a table of percentages from it for showing 
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
        # retrieving the line chart, heat map of regions ranked by absence rate, and ranking of regions by average absence rate before display them
        fig1, fig2, rankings = getAnalysis(data)
        print("Displaying Absence Rate Line Chart...")
        fig1.show()
        print("Displaying Region Ranking by Absence Rate Heat Map...")
        fig2.show()
        res = spark.createDataFrame(rankings, ["rank (best -> worst)", "region_name"])
        print("Ranking of Average Absence Rate Per Region from 2006-2018")
        res.show(res.count(), False)

    # asking the user if they have another query to ask
    print()
    loop = [inquirer.List('loop', message='Continue?', choices=["Yes", "No"], carousel=True)]
    loop = True if inquirer.prompt(loop)['loop'] == "Yes" else False