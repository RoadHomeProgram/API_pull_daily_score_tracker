#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 29 11:35:57 2022

@author: ryanschubert
"""
import requests
import pandas as pd
from datetime import date,timedelta
import numpy as np
from plotnine import ggplot,aes,geom_line,geom_point,theme_bw,labs,xlab,ylab,scale_colour_manual
import os

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
    relevant_records=df[['mrn','redcap_event_name','cohort','pcl5_score','pcl5_score_pastmonth','phq9_score','phq9s_score','ptci_score']]
    relevant_records=relevant_records.assign(pcl5_score_all=np.where(relevant_records['pcl5_score']=="",relevant_records['pcl5_score_pastmonth'],relevant_records['pcl5_score']))
    relevant_records=relevant_records.loc[relevant_records['redcap_event_name'].str.contains('arm_1')]
    relevant_records=relevant_records.replace("",np.NaN)
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
    #pcl
    pcl_tmp=tmp.loc[tmp['pcl5_score_all'] == tmp['pcl5_score_all']]
    time_cat=pd.Categorical(pcl_tmp['redcap_event_name'], categories=["Baseline",
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
                "12Mon"])
    pcl_tmp=pcl_tmp.assign(time_cat=time_cat)
    
    gPCL=(ggplot(pcl_tmp,aes(x="time_cat",y="pcl5_score_all",group="mrn",colour="mrn")) + 
     geom_point()+
     geom_line()+
     theme_bw() +
     ylab("PCL-5 Score") +
     xlab("Measurement Time Point") +
     scale_colour_manual(values='#1D6F42') +
     labs(colour="MRN"))
    #phq
    phq_tmp=tmp.loc[tmp['phq9_score'] == tmp['phq9_score']]
    time_cat=pd.Categorical(phq_tmp['redcap_event_name'], categories=["Baseline",
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
                "12Mon"])
    phq_tmp=phq_tmp.assign(time_cat=time_cat)
    
    gPHQ=(ggplot(phq_tmp,aes(x="time_cat",y="phq9_score",group="mrn",colour="mrn")) + 
     geom_point()+
     geom_line()+
     theme_bw() +
     ylab("PHQ-9 Score") +
     xlab("Measurement Time Point") +
     scale_colour_manual(values='#1D6F42') +
     labs(colour="MRN"))
    #phqs
    phqs_tmp=tmp.loc[tmp['phq9s_score'] == tmp['phq9s_score']]
    time_cat=pd.Categorical(phqs_tmp['redcap_event_name'], categories=["Baseline",
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
                "12Mon"])
    phqs_tmp=phqs_tmp.assign(time_cat=time_cat)
    
    gPHQs=(ggplot(phq_tmp,aes(x="time_cat",y="phq9s_score",group="mrn",colour="mrn")) + 
     geom_point()+
     geom_line()+
     theme_bw() +
     ylab("PHQ-9s Score") +
     xlab("Measurement Time Point") +
     scale_colour_manual(values='#1D6F42') +
     labs(colour="MRN"))
    #ptci
    ptci_tmp=tmp.loc[tmp['ptci_score'] == tmp['ptci_score']]
    time_cat=pd.Categorical(ptci_tmp['redcap_event_name'], categories=["Baseline",
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
                "12Mon"])
    ptci_tmp=ptci_tmp.assign(time_cat=time_cat)
    
    gPTCI=(ggplot(ptci_tmp,aes(x="time_cat",y="ptci_score",group="mrn",colour="mrn")) + 
     geom_point()+
     geom_line()+
     theme_bw() +
     ylab("PTCI Score") +
     xlab("Measurement Time Point") +
     scale_colour_manual(values='#1D6F42') +
     labs(colour="MRN"))
    return gPCL, gPHQ, gPHQs, gPTCI, tmp


def lookup_cohort(mrn,df):
    cohort=df.loc[df['mrn']==mrn]['cohort'].dropna().reset_index(drop=True)[0]
    return cohort

#lookup outdir or create one if none exists
def get_outpath(root_out,mrn,cohort_num):
    if not root_out.endswith("/"):
        root_out=root_out + "/"
    outpath=root_out + "COHORT" + "_" + cohort_num +"/" + mrn + "/"
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

def main(apiToken,apiURL,root_out):
    preverify_SSL()
    mrns=identify_last_day_updates(apiToken,apiURL)
    records=extract_complete_record_set(mrns=mrns,apiToken=apiToken,apiURL=apiURL)
    records=clean_records(df=records)
    mrns=records.mrn.unique()
    for mrn in mrns:
        print(mrn)
        cohort_number=lookup_cohort(mrn=mrn,df=records)
        outpath=get_outpath(root_out=root_out,mrn=mrn,cohort_num=cohort_number)
        gPCL, gPHQ, gPHQs, gPTCI, dat=create_plot_obj(mrn=mrn,df=records)
        dat=dat.drop('pcl5_score_all',axis=1)
        dat.to_csv(outpath + mrn + "_summary.csv")
        if dat.shape[0] > 1 :
            gPCL.save(filename=outpath + mrn + "_PCL-5.png",dpi=300,height=4.9,width=7)
            gPHQ.save(filename=outpath + mrn + "_PHQ-9.png",dpi=300,height=4.9,width=7)
            gPHQs.save(filename=outpath + mrn + "_PHQ-9s.png",dpi=300,height=4.9,width=7)
            gPTCI.save(filename=outpath + mrn + "_PTCI.png",dpi=300,height=4.9,width=7)
