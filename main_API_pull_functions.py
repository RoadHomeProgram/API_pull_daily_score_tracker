#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 29 11:35:57 2022

@author: ryanschubert
"""
import requests
import pandas as pd
from datetime import date,timedelta,datetime
import numpy as np
import os
import seaborn as sns
import shutil 

def identify_last_day_updates(apiToken,apiURL): #this function identifies each unique MRN that either has an updated response action or a created record action in the last 24 hours
    last_24_hours=str(date.today()-timedelta(days=1)) + " 00:00"
    data = {
        'token': apiToken,
        'content': 'log',
        'logtype': '',
        'user': '',
        'record': '',
        'beginTime': last_24_hours,
        'endTime': '',
        'format': 'json',
        'returnFormat': 'json'
    }
    r = requests.post(apiURL,data=data)
    r.raise_for_status()
    tmp=pd.DataFrame(r.json())
    tmp=tmp[tmp['action'].str.contains("Updated Response|Created Record")]
    mrn=tmp['action'].str.replace("Updated Response|Created Record|(Auto calculation)","").str.replace("(","").str.replace(")","").str.strip().unique()
    return mrn

def extract_data(mrn,apiToken,apiURL): #this function extracts all exisiting pcl,ptci, and phq redcap data for a given mrn
    data = {
        'token': apiToken,
        'content': 'record',
        'action': 'export',
        'format': 'json',
        'type': 'flat',
        'csvDelimiter': '',
        'forms[0]': 'participant_information',
        'forms[1]': 'pcl_past_month',
        'forms[2]': 'phq9',
        'forms[3]': 'ptci',
        'forms[4]': 'pcl_past_week',
        'forms[5]': 'participant_validation',
        'records[0]': mrn,
        'events[0]': 'baseline_survey_arm_1',
        'events[1]': 'monday_week_1_arm_1',
        'events[2]': 'tuesday_week_1_arm_1',
        'events[3]': 'wednesday_week_1_arm_1',
        'events[4]': 'thursday_week_1_arm_1',
        'events[5]': 'friday_week_1_arm_1',
        'events[6]': 'monday_week_2_arm_1',
        'events[7]': 'tuesday_week_2_arm_1',
        'events[8]': 'wednesday_week_2_arm_1',
        'events[9]': 'thursday_week_2_arm_1',
        'events[10]': 'posttreatment_ques_arm_1',
        'events[11]': '1_month_followup_arm_1',
        'events[12]': '3_month_followup_arm_1',
        'events[13]': '6_month_followup_arm_1',
        'events[14]': '12_month_followup_arm_1',
        'events[15]': 'preiop_family_asse_arm_2',
        'events[16]': 'postiop_family_ass_arm_2',
        'events[17]': 'baseline_survey_arm_3',
        'events[18]': 'monday_week_1_arm_3',
        'events[19]': 'tuesday_week_1_arm_3',
        'events[20]': 'wednesday_week_1_arm_3',
        'events[21]': 'thursday_week_1_arm_3',
        'events[22]': 'friday_week_1_arm_3',
        'events[23]': 'monday_week_2_arm_3',
        'events[24]': 'tuesday_week_2_arm_3',
        'events[25]': 'wednesday_2_arm_3',
        'events[26]': 'thursday_week_2_arm_3',
        'events[27]': 'posttreatment_ques_arm_3',
        'events[28]': '3_month_followup_arm_3',
        'events[29]': '6_month_followup_arm_3',
        'events[30]': '12_month_followup_arm_3',
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }
    
    r = requests.post(apiURL,data=data)
    r.raise_for_status()
    rD=pd.DataFrame(r.json())
    return rD

def extract_complete_record_set(mrns,apiToken,apiURL):
    complete_records=pd.DataFrame()
    for mrn in mrns:
        tmp=extract_data(mrn,apiToken,apiURL)
        complete_records=complete_records.append(tmp)
    complete_records.reset_index(drop=True,inplace=True)
    return complete_records

def clean_records(df):
    relevant_records=df[['mrn','first_name','last_name','date','redcap_event_name','cohort','pcl5_score','pcl5_score_pastmonth','phq9_score','phq9s_score','ptci_score']]
    relevant_records=relevant_records.assign(pcl5_score_all=np.where(relevant_records['pcl5_score']=="",relevant_records['pcl5_score_pastmonth'],relevant_records['pcl5_score']))
    relevant_records=relevant_records.loc[relevant_records['redcap_event_name'].str.contains('arm_1')]
    relevant_records=relevant_records.replace("",np.NaN)
    relevant_records['pcl5_score'] = relevant_records['pcl5_score'].astype(float)
    relevant_records['pcl5_score_pastmonth'] = relevant_records['pcl5_score_pastmonth'].astype(float)
    relevant_records['pcl5_score_all'] = relevant_records['pcl5_score_all'].astype(float)
    relevant_records['phq9_score'] = relevant_records['phq9_score'].astype(float)
    relevant_records['phq9s_score'] = relevant_records['phq9s_score'].astype(float)
    relevant_records['ptci_score'] = relevant_records['ptci_score'].astype(float)
    monovalue_records=relevant_records['mrn'].value_counts()[relevant_records['mrn'].value_counts()==1].index.tolist()
    relevant_records=relevant_records.loc[~relevant_records['mrn'].isin(monovalue_records)]
    return(relevant_records)

def create_plot_obj(mrn,df):
    tmp=df.loc[df['mrn'] == mrn]
    tmp=tmp.loc[tmp['redcap_event_name'].str.contains('arm_1')]

    condlist=[tmp['redcap_event_name'].str.contains("baseline"),
              tmp['redcap_event_name'].str.contains("monday_week_1_arm_1"),
              tmp['redcap_event_name'].str.contains("tuesday_week_1_arm_1"),
              tmp['redcap_event_name'].str.contains("wednesday_week_1_arm_1"),
              tmp['redcap_event_name'].str.contains("thursday_week_1_arm_1"),
              tmp['redcap_event_name'].str.contains("friday_week_1_arm_1"),
              tmp['redcap_event_name'].str.contains("monday_week_2_arm_1"),
              tmp['redcap_event_name'].str.contains("tuesday_week_2_arm_1"),
              tmp['redcap_event_name'].str.contains("wednesday_week_2_arm_1"),
              tmp['redcap_event_name'].str.contains("thursday_week_2_arm_1"),
              tmp['redcap_event_name'].str.contains("posttreatment_ques_arm_1"),
              tmp['redcap_event_name'].str.contains("1_month_followup_arm_1"),
              tmp['redcap_event_name'].str.contains("3_month_followup_arm_1"),
              tmp['redcap_event_name'].str.contains("6_month_followup_arm_1"),
              tmp['redcap_event_name'].str.contains("12_month_followup_arm_1")]
    choicelist=["Baseline",
                "Mon w1",
                "Tue w1",
                "Wed w1",
                "Thur w1",
                "Fri w1",
                "Mon w2",
                "Tue w2",
                "Wed w2",
                "Thur w2",
                "Post",
                "1Mon",
                "3Mon",
                "6Mon",
                "12Mon"]
    tmp['redcap_event_name']=np.select(condlist,choicelist,"null")
    dat=tmp.drop(['first_name','last_name','date','pcl5_score_all'],axis=1)
    
    tmp=tmp.drop(["pcl5_score",'pcl5_score_pastmonth','cohort','first_name','last_name','date'],axis=1)
    
    tmp=pd.melt(tmp,id_vars=['mrn','redcap_event_name'],value_vars=['phq9_score','phq9s_score', 'ptci_score', 'pcl5_score_all'])
#    tmp=tmp.loc[tmp['value'] == tmp['value']]
    condlist=[tmp['variable'].str.contains("pcl5_score_all"),
              tmp['variable'].str.contains("ptci_score"),
              tmp['variable'].str.contains("phq9_score"),
              tmp['variable'].str.contains("phq9s_score")]
    choicelist=["PCL-5",
                "PTCI",
                "PHQ-9",
                "PHQ-9s"]
    tmp['variable']=np.select(condlist,choicelist,"null")
    
    sns.set(rc={'figure.figsize':(28,20),'figure.dpi':(300)})
    g=sns.FacetGrid(tmp,col="variable",sharey=False,col_wrap=2)
    g.axes[0].set_ylim(0,27)
    g.axes[1].set_ylim(0,4)
    g.axes[2].set_ylim(0,250)
    g.axes[3].set_ylim(0,80)
    g.axes[0].set_xticklabels(g.axes[0].get_xticklabels(), rotation=45)
    g.axes[1].set_xticklabels(g.axes[1].get_xticklabels(), rotation=45)
    g.axes[2].set_xticklabels(g.axes[2].get_xticklabels(), rotation=45)
    g.axes[3].set_xticklabels(g.axes[3].get_xticklabels(), rotation=45)
    p=g.map_dataframe(sns.pointplot,x="redcap_event_name",y="value")
    p2 = g.map_dataframe(sns.lineplot,x="redcap_event_name",y="value",lw=2.5)
    p.set_titles('{col_name}')
    return p, dat
    
def lookup_cohort(mrn,df):
    cohort=df.loc[df['mrn']==mrn]['cohort'].dropna().reset_index(drop=True)[0]
    return cohort

def lookup_cohort_startdate(cohort_num,df):
    tmp=df.loc[(df['cohort']==cohort_num)]['mrn']
    tmp=df.loc[df['mrn'].isin(tmp)]
    tmp=tmp.loc[(tmp['redcap_event_name'] == "monday_week_1_arm_1")]['date']
    tmp=pd.to_datetime(tmp,format="%Y-%m-%d")
    sd=min(tmp).month_name() + " " + str(min(tmp).date().day)
    year=str(min(tmp).year)
    return sd, year

def lookup_initials(mrn,df):
    tmp=df.loc[df['mrn'] == mrn]
    tmp=tmp[['first_name','last_name']]
    tmp=tmp.loc[(tmp['first_name'] == tmp['first_name']) & (tmp['last_name'] == tmp['last_name'])]
    initials=tmp.iloc[0,0][0] + tmp.iloc[0,1][0]
    return(initials)
#lookup outdir or create one if none exists
def get_outpath(root_out,mrn,cohort_num,sd,year,initials):
    if not root_out.endswith("/"):
        root_out=root_out + "/"
    outpath=root_out + year + "/" + sd + "/" + "COHORT" + "_" + cohort_num +"/" + initials + "_" + mrn + "/"
    if not os.path.isdir(outpath):
        os.makedirs(outpath)
    return outpath

def preverify_SSL():
    try:
        session = requests.Session()
        response = session.get('https://redcap.rush.edu/redcap/',allow_redirects=False,verify=True)
    except requests.exceptions.SSLError:
        print("Error: problems verifying SSL ")
        print(requests.exceptions.SSLError)
        exit()

def archive_previous(root_out,mrn,cohort_num,sd,year,initials):
    timestamp="archival_timestamp_" + datetime.now().strftime("%m.%d.%Y_%H.%M")
    if not os.path.isdir(root_out + year + "/" + sd + "/" + "COHORT" + "_" + cohort_num +"/" + initials + "_" + mrn + "/archive/"):
        os.makedirs(root_out + year + "/" + sd + "/" + "COHORT" + "_" + cohort_num +"/" + initials + "_" + mrn + "/archive/")
    os.makedirs(root_out + year + "/" + sd + "/" + "COHORT" + "_" + cohort_num +"/" + initials + "_" + mrn + "/archive/" + initials + "_" + mrn + "_" +timestamp)
    png_out=root_out + year + "/" + sd + "/" + "COHORT" + "_" + cohort_num +"/" + initials + "_" + mrn + "/" + initials + "_" + mrn + ".png"
    csv_out=root_out + year + "/" + sd + "/" + "COHORT" + "_" + cohort_num +"/" + initials + "_" + mrn + "/" + initials + "_" + mrn + "_summary.csv"
    if os.path.isfile(png_out):
        shutil.move(png_out, root_out + year + "/" + sd + "/" + "COHORT" + "_" + cohort_num +"/" + initials + "_" + mrn + "/archive/" + initials + "_" + mrn + "_" + timestamp+ "/" + initials + "_" + mrn + ".png")
    if os.path.isfile(csv_out):
        shutil.move(csv_out, root_out + year + "/" + sd + "/" + "COHORT" + "_" + cohort_num +"/" + initials + "_" + mrn + "/archive/" + initials + "_" + mrn + "_" + timestamp+ "/" + initials + "_" + mrn + "_summary.csv")
    if len(os.listdir(root_out + year + "/" + sd + "/" + "COHORT" + "_" + cohort_num +"/" + initials + "_" + mrn + "/archive/" + initials + "_" + mrn + "_" + timestamp + "/")) != 0 :
        shutil.make_archive(root_out + year + "/" + sd + "/" + "COHORT" + "_" + cohort_num +"/" + initials + "_" + mrn + "/archive/" + initials + "_" + mrn + "_" + timestamp + ".zip",'zip',root_out + year + "/" + sd + "/" + "COHORT" + "_" + cohort_num +"/" + initials + "_" + mrn + "/archive/" + initials + "_" + mrn + "_" + timestamp + "/")
        shutil.rmtree(root_out + year + "/" + sd + "/" + "COHORT" + "_" + cohort_num +"/" + initials + "_" + mrn + "/archive/" + initials + "_" + mrn + "_" + timestamp + "/")
    
def main(apiToken,apiURL,root_out):
    preverify_SSL()
    mrns=identify_last_day_updates(apiToken,apiURL)
    records=extract_complete_record_set(mrns=mrns,apiToken=apiToken,apiURL=apiURL)
    records=clean_records(df=records)
    mrns=records.mrn.unique()
    for mrn in mrns:
        print(mrn)
        cohort_number=lookup_cohort(mrn=mrn,df=records)
        start,year=lookup_cohort_startdate(cohort_num=cohort_number,df=records)
        initials=lookup_initials(mrn=mrn,df=records)
        outpath=get_outpath(root_out=root_out,mrn=mrn,cohort_num=cohort_number,sd=start,year=year,initials=initials)
        archive_previous(root_out=root_out,mrn=mrn,cohort_num=cohort_number,sd=start,year=year,initials=initials)
        p,dat=create_plot_obj(mrn=mrn,df=records)
        dat.to_csv(outpath + initials + "_" + mrn + "_summary.csv")
        p.savefig(fname=outpath + initials + "_" + mrn + ".png")
