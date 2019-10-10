from django.http import HttpResponseRedirect
from django.shortcuts import render,get_object_or_404
from .forms import CSVFileForm, ProjectForm
from .models import CSVFile, Project, CSVstatistics, ProjectMonitor
import pandas as pd
from mysite.settings import BASE_DIR
from django.urls import reverse
from django.contrib.auth.models import User, Group

from django.http import *
from django.shortcuts import render_to_response,redirect
from django.template import RequestContext
from django.contrib.auth.decorators import login_required
from django.contrib.auth import authenticate, login, logout
from django.views.decorators.csrf import csrf_exempt
from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework import viewsets
from .serializers import UserSerializer
from django.views.generic.edit import CreateView
from django.utils import timezone
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from django.urls import reverse
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.functions import isnan, when, count, col
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from IPython.display import display, HTML

from os.path import abspath
import re
import os
import csv
import pyspark.sql.functions as f
import json
from collections import Counter
from collections import ChainMap
from django.core.paginator import Paginator
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.ml.feature import FeatureHasher
from pyspark.ml.feature import StandardScaler
@csrf_exempt
def redirect(request):
    return HttpResponseRedirect("index")

@csrf_exempt
def redirectCsv(request, project_id):
    form = CSVFileForm()
    return HttpResponseRedirect("uploadCSVFile",{'form':form, 'project_id':project_id } )
    

@csrf_exempt
def header(request):
    return render(request,'Spark_poc/header.html')

@csrf_exempt
def footer(request):
    return render(request,'Spark_poc/footer.html')

@csrf_exempt
def projcreation (request):
    if request.method == "POST":
        print("projcreation")
        form = ProjectForm(request.POST,request.FILES)
        if "Cancel" in request.POST:
            query_results = Project.objects.all()
            print(query_results)
            return HttpResponseRedirect("index")
        else:
            project1 = Project(project_name=request.POST['project_name'], description=request.POST['description'], task=request.POST['task'], status=request.POST['status'], created_date=timezone.now())
            project1.save()
            print(project1)
            query_results = Project.objects.all()
            print(query_results)  
    return HttpResponseRedirect("index")
    #return render_to_response('Spark_poc/homepage.html', {'existingProject':query_results})
        #return render(request,'Spark_poc/fileUpload.html')



@csrf_exempt
def deleteProject(request, project_id):
    print('inside delete')
    print(project_id)
    #formFile = CSVFile.objects.all().prefetch_related('project_fk')
    #formFile = CSVFile.objects.all().get(project_fk = project_id)
    
    formFile =  CSVFile.objects.filter(project_fk = project_id)
    print(formFile)
    if formFile.count()==0:
        print('inside form')
        projectdelete =  get_object_or_404(Project, id=project_id).delete()
        query_results = Project.objects.all()
        print(query_results)
    else:
        print(formFile.count())
        for f in formFile:
            filePath = BASE_DIR+'\\'+str(f.file)
            print(filePath)
            if os.path.isfile(filePath):
                os.remove(filePath)
                print(formFile)
                filedelete = get_object_or_404(CSVFile, id=f.id).delete()
                projectdelete =  get_object_or_404(Project, id=project_id).delete()
        query_results = Project.objects.all()
        print(query_results) 
    return render_to_response('Spark_poc/homepage.html',{'existingProject':query_results})
    #render_to_response('deletebook.html',{'books':books},context_instance=RequestContext(request))
    #return HttpResponseRedirect("index")
    



@csrf_exempt
def deleteDataSet(request,csvfile_id):
    print('insideDelete')
    formFile = CSVFile.objects.get(id = csvfile_id)
    print(formFile)
    filePath = BASE_DIR+'\\'+str(formFile.file)
    print(filePath)
    if os.path.isfile(filePath):
        os.remove(filePath)
    print(formFile)
    filedelete = get_object_or_404(CSVFile, id=csvfile_id).delete()
    print(formFile.file)
    project_id = formFile.project_fk.id
    #filename = formFile.file
    

    # DataSet = get_object_or_404(CSVFile, id=csvfile_id).delete()
    #print(DataSet)
    return render(request,'Spark_poc/uploadCSV.html', {'project_id':project_id})

@csrf_exempt
def fileUploadPage(request, project_id):
   print ('fileUploadPage')
   form = CSVFileForm()
   #return redirectCsv(request, project_id)
   return render(request,'Spark_poc/fileUpload.html', {'form':form, 'project_id':project_id })
   #return render(request,'Spark_poc/fileUpload.html', {'form':form, 'project_id':project_id })

@csrf_exempt
def uploadfile(request, project_id):
    if request.method == "POST":
        print ('inside uploadfile')
        print(project_id)
        form = CSVFileForm(request.POST,request.FILES)
        if "Cancel" in request.POST:
            print("canceled") 
            query_result = CSVFile.objects.filter(project_fk=project_id)
            print(query_result)
            #return redirectCsv(request, project_id)
            return render(request,'Spark_poc/uploadCSV.html', {'existingCSV':query_result, 'project_id':project_id})
        else:
            if form.is_valid():
                instance = CSVFile(file=request.FILES['file']);
                instance.name = request.POST['name'];
                print("ddddddd")
                instance.purpose = request.POST['purpose'];
               
                #print(Project.objects.filter(id=project_id))
                print(get_object_or_404(Project, pk=project_id))
                projInst = get_object_or_404(Project, pk=project_id)
                instance.project_fk = projInst
                #instance.project_fk =  Project.objects.filter(id=project_id)
                #instance.project_fk =  project_id
                instance.save()
                print (instance.id)
                query_result = CSVFile.objects.filter(project_fk=project_id)
                print(query_result)
                return render(request,'Spark_poc/uploadCSV.html', {'existingCSV':query_result, 'project_id':project_id})
            else:
                print('elseeee')
                return render(request,'Spark_poc/error.html', {'errormsg':"File upload failed"})
    else:
        print ('else')
        form = CSVFileForm()
        return render(request,'Spark_poc/fileUpload.html', {'form':form})


@csrf_exempt
def getTable(request,csvfile_id):
    print('data preprocessing')
    formFile = get_object_or_404(CSVFile, pk=csvfile_id)
    print(formFile)
    filePath = BASE_DIR+'\\'+str(formFile.file)
    projectid = formFile.project_fk.id
    print(formFile.project_fk.id)
    filename = filePath
    print (filename)
    print(formFile.id)
    spark=sparkSession(request)
    print("Created Spark Session")
    with open(filePath, 'r') as readFile:
        sn = csv.Sniffer()
        has_header = sn.has_header((readFile.read()))
        print(has_header)
        readFile.seek(0)

        if has_header:
            df = spark.read.csv(filePath,header=True,sep="," ,inferSchema=True)
            header = df.columns
            print(header)
            columns = df.toJSON().collect()
            #print(columns)
            columns = json.dumps("testing")
            column = json.loads(columns)

           
            print(column)
            

            
            #values = dictionary.values()
            #columnload = json.loads(columns)
            #formatted_table = (json2html.convert(json = columnload))
    #return render(request,'Spark_poc/csvfile.html', {'header':header,'columns':column})

@csrf_exempt
def getStatisticsData(request, csvfile_id):
    print ('into getStatisticsData')
    formFile = get_object_or_404(CSVFile, pk=csvfile_id)
    filePath = BASE_DIR+'\\'+str(formFile.file)
    #print(formFile.project_fk.id)
    projectid = formFile.project_fk.id
    filename = filePath
    fileid = formFile.id 
    #print (filePath)
    #print(formFile.headers)
    print(formFile.id)
    query_res = CSVstatistics.objects.all().filter(csvfile_fk=formFile.id)


    if query_res.exists():
        print("query_res exists")
        samples = query_res.values_list('samples', flat=True)
        samples = samples.first()
        attributes = query_res.values_list('attributes', flat=True)
        attributes = attributes.first()
        if ((query_res.values_list('freezed_data',flat=True)).first()) != "":
            print("not empty")
            arrdata1 = query_res.values_list('freezed_data',flat=True)
            arrdata1 = arrdata1.first()
            arrdata = tuple(eval(arrdata1))
        else:
            print("empty")
            arrdata1 = query_res.values_list('statisticsdata',flat=True)
            arrdata1 = arrdata1.first()
            arrdata = tuple(eval(arrdata1))

        statsid = query_res.values_list('id',flat=True)
        statsid = statsid.first()
        #arrdata = json.loads(arrdata)
        #print(type(arrdata))


    else:
        print("else")
        spark=sparkSession(request)
        print("Created Spark Session")
        with open(filePath, 'r') as readFile:
            sn = csv.Sniffer()
            has_header = sn.has_header((readFile.read()))
            print(has_header)
            readFile.seek(0)

        if has_header:
            print(formFile.has_headers)
            formFile.has_headers = "true"
            formFile.save()
            df = spark.read.csv(filePath,header=True,sep="," ,inferSchema=True)
            spark.sql('DROP TABLE IF EXISTS TempTable')
            spark.sql('create database IF NOT EXISTS hivedb')
            spark.sql('use hivedb')
            df.createOrReplaceTempView('TempTable')
            csvpath = filename
            seperate = csvpath.split('/')
            for temp in seperate:
                pass
            splitcsv = temp.split('.')
            csvname = splitcsv[0]
            fid= str(fileid)
            pid = str(projectid)
            tablename=csvname+ '_' + fid +'_' + pid
            print(tablename)
            #query_results = ProjectMonitor.objects.all()
            #print(query_results)  
            a = "create table IF NOT EXISTS "+str(tablename)+" as select * from TempTable"
            datapreprocess  = tablename+ '_prerocessing'
            print(datapreprocess)
            spark.sql("create table IF NOT EXISTS "+str(datapreprocess)+" as select * from TempTable")
            spark.sql("select * from "+str(datapreprocess)+"").show()
            spark.sql('show tables').show()
            #spark.sql('select * from calhousing').show()
            samples = df.count() #total number of record
            attributes = len(df.columns) #total number of column
            header = df.columns
            print(header)
            arrdata = []
            a=0
            for dfcolumn in header:
                print (dfcolumn)
                
                nullvalues = df.where(col(dfcolumn).isNull()).count()
                
                item = {"feature": dfcolumn}
                print(dict(df.dtypes)[dfcolumn] == "string")
                print(df.select(dfcolumn).distinct().count() < 500)
                if dict(df.dtypes)[dfcolumn] == "string" and df.select(dfcolumn).distinct().count() < 500 :
                    item["type"] = "categorial"
                else:
                    item["type"] = dict(df.dtypes)[dfcolumn]
                print(item["type"])
                print(dict(df.dtypes)[dfcolumn])
                print(df.select(dfcolumn).distinct().count())
                item["unique"] = df.select(dfcolumn).distinct().count()
                item["missing"] = ('%.4f' % (((nullvalues)/(float(df.select(dfcolumn).count())))*100))
                st = df.describe([dfcolumn]).collect()
                statarray = []
                for i in st:
                    statvalues = {i.summary:i.__getitem__(dfcolumn)}
                    statarray.append(statvalues)

                if dict(df.dtypes)[dfcolumn] != "string":
                    medianvalues = {"median":(df.approxQuantile(df.columns[a], [0.5], 0.001))[0]}
                    statarray.append(medianvalues)


                item["stats"] = statarray 
                item["usage"] = "" 
                arrdata.append(item)
                a+=1     

            #cc = json.dumps(arrdata)
            #cc = json.loads(cc)
            #print(cc)

        else:
            formFile.has_headers = "false"
            formFile.save()
            df = spark.read.csv(filePath,sep="," ,inferSchema=True)
            print(df.show())
            spark.sql('create database IF NOT EXISTS hivedb')
            spark.sql('use hivedb')
            df.createOrReplaceTempView('TempTable')
            csvpath = filename
            seperate = csvpath.split('/')
            for temp in seperate:
                pass
            splitcsv = temp.split('.')
            csvname = splitcsv[0]
            fid= str(fileid)
            pid = str(projectid)
            tablename=csvname+ '_' + fid +'_' + pid
            print(tablename)
            spark.sql("DROP TABLE IF EXISTS "+str(tablename)+"")
            a = "create table IF NOT EXISTS "+str(tablename)+" as select * from TempTable"
            datapreprocess  = tablename+ '_prerocessing'
            print(datapreprocess)
            spark.sql("create table IF NOT EXISTS "+str(datapreprocess)+" as select * from TempTable")
            spark.sql("select * from "+str(datapreprocess)+"").show()
            spark.sql('show tables').show()
            samples = df.count() #total number of record
            attributes = len(df.columns) #total number of column
            header = df.columns
            arrdata = []
            a=0
            for dfcolumn in header:
                print (dfcolumn)
                
                nullvalues = df.where(col(dfcolumn).isNull()).count()
                
                item = {"feature": dfcolumn}
                print(dict(df.dtypes)[dfcolumn] == "string")
                print(df.select(dfcolumn).distinct().count() < 500)
                if dict(df.dtypes)[dfcolumn] == "string" and df.select(dfcolumn).distinct().count() < 500 :
                    item["type"] = "categorial"
                else:
                    item["type"] = dict(df.dtypes)[dfcolumn]
                print(item["type"])
                print(dict(df.dtypes)[dfcolumn])
                item["unique"] = df.select(dfcolumn).distinct().count()-1
                item["missing"] = ('%.4f' % (((nullvalues)/(float(df.select(dfcolumn).count())))*100))
                st = df.describe([dfcolumn]).collect()
                statarray = []
                for i in st:
                    statvalues = {i.summary:i.__getitem__(dfcolumn)}
                    statarray.append(statvalues)

                if dict(df.dtypes)[dfcolumn] != "string":
                    medianvalues = {"median":(df.approxQuantile(df.columns[a], [0.5], 0.001))[0]}
                    statarray.append(medianvalues)

                item["stats"] = statarray
                item["usage"] = ""
                arrdata.append(item)
                a+=1     

            #cc = json.dumps(arrdata)
            #cc = json.loads(cc)
            #print(cc)
        #print(arrdata)
        csvstats = CSVstatistics(samples = samples, attributes = attributes, csvfile_fk = formFile, statisticsdata = str(arrdata))
        csvstats.save()
        statsid = csvstats.id
        print(csvstats.id)
        print(csvstats)
        print(arrdata)
    return render(request,'Spark_poc/data.html', {'samples':samples, 'attributes':attributes, 'cc':arrdata, 'projectid': projectid, 'filename':filename, 'fileid':fileid, 'statsid':statsid }, content_type="text/html")

#csrf_exempt
#def login_user(request):
    #logout(request)
    #print(request)
    #print(request.POST['user_name'])
    #response = Response(
            #{"detail": "This action is not authorized"},
            #content_type="application/json",
            #status=400,
        #)
    #return render(request,'Spark_poc/project.html')
#@csrf_exempt
#def login_user(request):
    #logout(request)
    #username = password = ''
    #if request.method == 'POST':
        #username = request.POST['username']
        #password = request.POST['password']

        #user = authenticate(username=username, password=password)
        #print("Welcome ",str(authenticate(username=username, password=password)))
        #if user is not None:
            #if user.is_active:
                #login(request, user)
                #return render(request, 'Spark_poc/home.html',{'loggedby':str(user)})
        #else:
            #print('Please Enter Valid User Credientials')
            #return render(request,'Spark_poc/login.html', {'usernotactive':"Please Enter Valid User Credientials"})
    #load this html file, when we try to access the login page first time
    #print("Passing into login html")
    #return render(request,'Spark_poc/login.html')
@csrf_exempt
def project(request):
    print("Inside Project")
    query_results = Project.objects.all()
    print(query_results)
    return render(request,'Spark_poc/homepage.html', {'existingProject':query_results})

@csrf_exempt
def csvButton(request, project_id):
    print("Inside CSVFile")
    print(project_id )
    query_result = CSVFile.objects.filter(project_fk=project_id)
    if query_result.exists():
        print(query_result)
        return render(request,'Spark_poc/uploadCSV.html', {'existingCSV':query_result, 'project_id':project_id })
    else:
        print("CSVFile is empty")
        return render(request,'Spark_poc/uploadCSV.html', {'project_id':project_id})

@csrf_exempt
def projcr(request):
    print("Inside projcr")
    return render(request,'Spark_poc/project.html')
def trainandtuning(request):
    print("Page Inprogress")
    return render(request,'Spark_poc/trainandtuning.html')
def prediction(request):
    print("Page Inprogress")
    return render(request,'Spark_poc/prediction.html')
def result(request):
    print("Page Inprogress")
    return render(request,'Spark_poc/result.html')
def logout (request):
    print("logout")
def sparkSession(request):
    print("into SparkSession")
    warehouse_location = abspath('spark-warehouse')
    spark = SparkSession.builder.appName("Spark_poc Application").master("local[*]").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()
    return spark

#class UserViewSet(viewsets.ModelViewSet):
    #print("lll")
    #serializer_class = UserSerializer
    #lookup_field = 'username'
    #queryset = User.objects.all(username=username)
    #print(queryset)
    #response = Response(
            #{"detail": "This action is not authorized"},
            #content_type="application/json",
            #status=400,
        #)
@csrf_exempt
def datapreprocess(request, file_id):
    print("into datapreprocess")
    formFile = get_object_or_404(CSVFile, id=file_id)
    projectid = formFile.project_fk.id 
    return render(request,'Spark_poc/result.html',{'file_id':file_id, 'projectid':projectid})

@csrf_exempt
def duplicate(request, file_id):
    print("into duplicate")
    spark=sparkSession(request)
    print("Created Spark Session")
    spark.sql('use hivedb')
    formFile = get_object_or_404(CSVFile, id=file_id)
    filePath = BASE_DIR+'\\'+str(formFile.file)
    filename = filePath
    projectid = formFile.project_fk.id 
        
    csvpath = filename
    seperate = csvpath.split('/')
    for temp in seperate:
        pass
    splitcsv = temp.split('.')
    csvname = splitcsv[0]
    fid= str(file_id)
    pid = str(projectid)
    tablename=csvname+ '_' + fid +'_' + pid
    
    datapreprocess =  tablename+ '_prerocessing'
    print(datapreprocess)
    #spark.sql("DROP TABLE IF EXISTS "+str(duplicatetable)+"")
    
    spark.sql("select * from "+str(datapreprocess)+"").show()
    df1= spark.table(datapreprocess)
    print(df1.count())
    print((df1.count())-(df1.distinct().count()))
    dups_dict = ((df1.count())-(df1.distinct().count()))
    header = df1.columns
    if "outliersValue" in header: header.remove("outliersValue")
    df1 = df1.drop('outliersValue')
    csvdata = df1.toJSON().collect()
    value = "duplicate"
    return render(request,'Spark_poc/result.html',{'dups_dict':dups_dict, 'header':header, 'file_id':file_id, 'projectid':projectid, 'data':csvdata, 'tab':value })


@csrf_exempt
def missing(request, file_id):
    print("into missing")
    spark=sparkSession(request)
    print("Created Spark Session")
    spark.sql('use hivedb')
    formFile = get_object_or_404(CSVFile, id=file_id)
    filePath = BASE_DIR+'\\'+str(formFile.file)
    filename = filePath
    projectid = formFile.project_fk.id 
        
    csvpath = filename
    seperate = csvpath.split('/')
    for temp in seperate:
        pass
    splitcsv = temp.split('.')
    csvname = splitcsv[0]
    fid= str(file_id)
    pid = str(projectid)
    tablename=csvname+ '_' + fid +'_' + pid
    print(tablename)
    datapreprocess =  tablename+ '_prerocessing'    
    spark.sql("select * from "+str(datapreprocess)+"").show()
    df1= spark.table(datapreprocess)
    header = df1.columns
    if "outliersValue" in header: header.remove("outliersValue")
    df1 = df1.drop('outliersValue')
    csvdata = df1.toJSON().collect()
    arrdata = []
    for dfcolumn in header:
                #print (dfcolumn)
                nullvalues = df1.where(col(dfcolumn).isNull()).count()
                item = ('%.4f' % (((nullvalues)/(float(df1.select(dfcolumn).count())))*100))
                #item["missing"] = ('%.4f' % (((nullvalues)/(float(df.select(dfcolumn).count())))*100))
                #print(item)
                arrdata.append(item)
    arrdata = zip(header,arrdata)
    print(arrdata)
    value = "missing"
    return render(request,'Spark_poc/result.html',{'header':header, 'file_id':file_id, 'projectid':projectid, 'arrdata':arrdata, 'data':csvdata, 'tab':value })

@csrf_exempt
def outlier(request, file_id):
    print("into outlier")
    spark=sparkSession(request)
    print("Created Spark Session")
    spark.sql('use hivedb')
    formFile = get_object_or_404(CSVFile, id=file_id)
    filePath = BASE_DIR+'\\'+str(formFile.file)
    filename = filePath
    projectid = formFile.project_fk.id 
        
    csvpath = filename
    seperate = csvpath.split('/')
    for temp in seperate:
        pass
    splitcsv = temp.split('.')
    csvname = splitcsv[0]
    fid= str(file_id)
    pid = str(projectid)
    tablename=csvname+ '_' + fid +'_' + pid
    print(tablename)
    datapreprocess =  tablename+ '_prerocessing'
    
    #spark.sql("select * from "+str(datapreprocess)+"").show()
    df= spark.table(datapreprocess)
    
    currentdf = df
    print(currentdf)
    #print(type(currentdf)) #'pyspark.sql.dataframe.DataFrame'
    header = df.columns
    print(header)
    currentdfwithoutstring = []
    print(currentdfwithoutstring)
    #print(type(header)) #<class 'list'>
    a = 0 #
    if "outliersValue" in header: 
        header.remove("outliersValue")
        a = 1
    else:
        spark.sql("ALTER TABLE "+str(datapreprocess)+" ADD COLUMNS (outliersValue String)").show()
        spark.sql("select * from "+str(datapreprocess)+"").show()
        a = 2


    bounds = {}
    
    for dfcolumn in header:
        print(dfcolumn)
        if dict(df.dtypes)[dfcolumn] != "string":
            quantiles = df.approxQuantile(
                dfcolumn, [0.25, 0.75], 0.05
            )
            print(quantiles[0])
            print(quantiles[1])
            IQR = quantiles[1] - quantiles[0]
            print(IQR)
            bounds[dfcolumn] = [
                    quantiles[0] - 1.5 * IQR, 
                    quantiles[1] + 1.5 * IQR
            ]
            print(bounds)
            currentdfwithoutstring.append(dfcolumn)

    print(currentdfwithoutstring)

    outliers = df.select(*[
        (
            (df[c] < bounds[c][0]) | 
            (df[c] > bounds[c][1])
        ).alias(c + '_o') for c in currentdfwithoutstring
    ])
    
    df = outliers.toPandas().any(axis=1)
    #print(type(df))
    dff = df.to_frame(name='outliersValue')
    print(dff)
    #if a==2:
    print("a is zero")
    #print('True' in dff.values)
    
    currentdf = currentdf.toPandas()
    print(currentdf)
    currentdf["outliersValue"] = dff
    print(currentdf)

    currentdfpandas = spark.createDataFrame(currentdf)
    currentdfpandas.write.insertInto("hivedb."+str(datapreprocess)+"",overwrite = True)
    # spark.sql("DROP TABLE IF EXISTS "+str(datapreprocess)+"")
    # currentdfpandas.createOrReplaceTempView('TempTableout')
    # spark.sql("create table "+str(datapreprocess)+" as select * from TempTableout")
    spark.sql("select * from "+str(datapreprocess)+"").show()
    spark.sql('show tables').show()
    
    ##spark.sql("UPDATE "+str(datapreprocess)+" SET outliersValue  =  as select * from TempTable").show()
    #spark.sql("insert overwrite table "+str(datapreprocess)+" partition(outliersValue = as select * from TempTable) select outliersValue from "+str(datapreprocess)+"").show()

    
    outliers = outliers.toPandas()
    #print(type(outliers))
    outliers["outliersValue"] = dff
    print(outliers)
    #print(outliers.take(1).isEmpty)
    print("first")
    outliers = outliers.query('outliersValue == True')
    print(outliers)
    print(type(outliers))
    if outliers.empty:
        print("empty")
        outliers = []

    else:
        print("not empty")
        outliers = outliers.drop(['outliersValue'], axis=1)
        print(outliers)
        outliers = spark.createDataFrame(outliers)
        outliers = outliers.toJSON().collect()
        print(type(outliers))
        
    print(outliers)
    value = "outliers"
    
    return render(request,'Spark_poc/result.html',{'header':currentdfwithoutstring, 'file_id':file_id, 'projectid':projectid, 'data':outliers, 'tab':value })

@csrf_exempt
def featurehasher(request):
    print("into featurehasher")
    value = "featurehasher"
    file_id = request.GET['fileid']
    print(file_id)
    spark=sparkSession(request)
    print("Created Spark Session")
    spark.sql('use hivedb')
    formFile = get_object_or_404(CSVFile, id=file_id)
    filePath = BASE_DIR+'\\'+str(formFile.file)
    filename = filePath
    projectid = formFile.project_fk.id 
        
    csvpath = filename
    seperate = csvpath.split('/')
    for temp in seperate:
        pass
    splitcsv = temp.split('.')
    csvname = splitcsv[0]
    fid= str(file_id)
    pid = str(projectid)
    tablename=csvname+ '_' + fid +'_' + pid
    print(tablename)
    datapreprocess =  tablename+ '_prerocessing'
    
    #spark.sql("select * from "+str(datapreprocess)+"").show()
    df= spark.table(datapreprocess)
    header = df.columns
    print(header)
    hasher = FeatureHasher(inputCols=header,
                       outputCol="features")

    featurized = hasher.transform(df)
    featurized.show(truncate=False)
    dff = featurized

    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                        withStd=True, withMean=False)

    # Compute summary statistics by fitting the StandardScaler
    scalerModel = scaler.fit(dff)

    # Normalize each feature to have unit standard deviation.
    scaledData = scalerModel.transform(dff)
    scaledData.show(truncate=False)

    return JsonResponse({"success":True }, status=200)

@csrf_exempt
def likePost(request):
    #unique missing and stats find from new hivedb
    if request.method == 'GET':
        print("likepost")
        usageArray = request.GET.getlist('usagearray[]')
        print(usageArray)
        headers = request.GET.getlist('headers[]')
        print(headers)

        statsid = request.GET['statsid']
        formStats = get_object_or_404(CSVstatistics, id=statsid)              
        statsdata = formStats.statisticsdata
        statsdata = eval(statsdata)
        file_id = request.GET['fileid']
        print(file_id)
        spark=sparkSession(request)
        print("Created Spark Session")
        spark.sql('use hivedb')

        formFile = get_object_or_404(CSVFile, id=file_id)
        filePath = BASE_DIR+'\\'+str(formFile.file)
        filename = filePath
        projectid = formFile.project_fk.id 
        print(projectid)    
        csvpath = filename
        seperate = csvpath.split('/')
        for temp in seperate:
            pass
        splitcsv = temp.split('.')
        csvname = splitcsv[0]
        fid= str(file_id)
        pid = str(projectid)
        tablename=csvname+ '_' + fid +'_' + pid
        print(tablename)
        datapreprocess =  tablename+ '_prerocessing'
        
        df= spark.table(datapreprocess)
        header = df.columns
        print(header)
        if "outliersValue" in header: header.remove("outliersValue")
        df = df.drop('outliersValue')

        samples = df.count() #total number of record
        attributes = len(df.columns) #total number of column

        # print(usageArray)
        a=0
        for statsusage in statsdata:
            featurevalue = statsusage['feature']
            headerindex = headers.index(featurevalue)
            usageArrayvalue =  usageArray[headerindex]

            st = df.describe([featurevalue]).collect()
            statarray = []
            for i in st:
                statvalues = {i.summary:i.__getitem__(featurevalue)}
                statarray.append(statvalues)

            if dict(df.dtypes)[featurevalue] != "string":
                print("not string")
                medianvalues = {"median":(df.approxQuantile(df.columns[a], [0.5], 0.001))[0]}
                statarray.append(medianvalues)


            statsusage['stats'] = statarray            
            statsusage['usage']=usageArrayvalue
            hasheaders = formFile.has_headers
            print(hasheaders)
            if hasheaders == "true":
                statsusage['unique'] = df.select(featurevalue).distinct().count()
            else:
                statsusage['unique'] = df.select(featurevalue).distinct().count()-1

            nullvalues = df.where(col(featurevalue).isNull()).count()
            statsusage["missing"] = ('%.4f' % (((nullvalues)/(float(df.select(featurevalue).count())))*100))
            #statsusage['usage']=usageArray[a]
            a+=1

        print(statsdata)
        formStats.freezed_data = statsdata
        formStats.samples = samples
        formStats.save()
        print(formStats.id)
        print(samples)
        print(attributes)
        #return HttpResponse("Success!") # Sending a success response
        #'samples':samples, 'attributes':attributes, 'cc':arrdata, 'projectid': projectid, 'filename':filename, 'fileid':file_id, 'statsid':statsid 
        return JsonResponse({"success":True }, status=200)
    else:
        print("else")
        return HttpResponse("Request method is not a GET")

@csrf_exempt
def dropduplicate(request):
    if request.method == 'GET':    
        print('inside dropduplicate')
        spark=sparkSession(request)
        print("Created Spark Session")
        fileid = request.GET['csvid']
        projectid = request.GET['projectid']
        formFile = get_object_or_404(CSVFile, id=fileid)
        filePath = BASE_DIR+'\\'+str(formFile.file)
        print(filePath)
        filename = filePath
        spark.sql('use hivedb')
        csvpath = filename
        seperate = csvpath.split('/')
        for temp in seperate:
            pass
        splitcsv = temp.split('.')
        csvname = splitcsv[0]
        fid= str(fileid)
        pid = str(projectid)
        tablename=csvname+ '_' + fid +'_' + pid
        print(tablename)
        datapreprocess =  tablename+ '_prerocessing'

        #spark.sql("DROP TABLE IF EXISTS "+str(duplicatetable)+"")
        duplicatetable = datapreprocess+ '_duplicate'
        spark.sql("insert overwrite table "+str(datapreprocess)+" select distinct * from "+str(datapreprocess)+"")
        monitor = ProjectMonitor(action="dropduplicate", query=" select distinct * from "+str(datapreprocess)+"",csvfileid=fid,tablename=tablename)
        monitor.save()
        print(monitor)
        query_results = ProjectMonitor.objects.all()
        print(query_results)  
        spark.sql("select * from "+str(datapreprocess)+"").show()
        df= spark.table(datapreprocess)
        dups_dict=(df.count()- df.distinct().count())
        print(dups_dict)
        header = df.columns
        if "outliersValue" in header: header.remove("outliersValue")
        df = df.drop('outliersValue')
        csvdata = df.toJSON().collect()
    return JsonResponse({"success":True ,'dups_dict':dups_dict,'data':csvdata,'header':header}, status=200)


@csrf_exempt
def dropOutliers(request):
    if request.method == 'GET':    
        print('inside dropOutliers')
        spark=sparkSession(request)
        print("Created Spark Session")
        fileid = request.GET['csvid']
        projectid = request.GET['projectid']
        formFile = get_object_or_404(CSVFile, id=fileid)
        filePath = BASE_DIR+'\\'+str(formFile.file)
        print(filePath)
        filename = filePath
        spark.sql('use hivedb')
        csvpath = filename
        seperate = csvpath.split('/')
        for temp in seperate:
            pass
        splitcsv = temp.split('.')
        csvname = splitcsv[0]
        fid= str(fileid)
        pid = str(projectid)
        tablename=csvname+ '_' + fid +'_' + pid
        print(tablename)
        datapreprocess =  tablename+ '_prerocessing'
        #duplicatetable = datapreprocess+ '_outliers'

        
        df= spark.table(datapreprocess)
        print(df)
        header = df.columns

        spark.sql("insert overwrite table "+str(datapreprocess)+" select * from "+str(datapreprocess)+" where outliersValue ='false'")
        
        #spark.sql("DELETE from "+str(datapreprocess)+" WHERE outliersValue = true")
        monitor = ProjectMonitor(action="dropOutliers", query=" select * from "+str(datapreprocess)+" where outliersValue ='false'",csvfileid=fid,tablename=tablename)
        monitor.save()
        print(monitor)
        query_results = ProjectMonitor.objects.all()
        print(query_results)  
        spark.sql("select * from "+str(datapreprocess)+"").show()
        spark.sql('show tables').show()
        
        #schema = StructType([StructField("k", StringType(), True), StructField("v", IntegerType(), False)])
        #outliers = spark.createDataFrame([],schema)
        if "outliersValue" in header: header.remove("outliersValue")
        #outliers = outliers.toJSON().collect()
        outliers = []
        print(outliers)

    return JsonResponse({"success":True , 'header':header, 'data':outliers}, status=200)
