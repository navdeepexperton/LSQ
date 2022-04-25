from flask import Flask,redirect
from datetime import datetime
from datetime import timedelta
import numpy as np
import pandas as pd
import gspread
import json
import requests
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials

app = Flask(__name__)
@app.route('/')
def home():
    today = datetime.today()
    yesterday = today - timedelta(days=3)
    today1 = today.strftime("%Y-%m-%d %H:%M:%S")
    yesterday1 = yesterday.strftime("%Y-%m-%d %H:%M:%S")
    Lead = pd.DataFrame()
    for i in range(1, 10):
        lead_squared_access_key = 'u$r9d756da850aa2e17ae61f497b2bc99c3'
        lead_squared_secret_key = '2b8db837b12d22937d2e95e2cb257af189ab0342'
        url = 'https://api-in21.leadsquared.com/v2/LeadManagement.svc/Leads.RecentlyModified?accessKey=%s&secretKey=%s' % (
        lead_squared_access_key, lead_squared_secret_key)
        headers = {"Content-Type": "application/json; charset=utf-8"}
        dic = {
            "Parameter": {
                "FromDate": yesterday1,
                "ToDate": today1
            },
            "Columns": {
                "Include_CSV": "FirstName,LastName,EmailAddress,Phone,CreatedOn,mx_CP_Code,mx_CP_Name,ProspectStage,mx_Last_Activity_Sub_Outcome,mx_Last_Activity_Outcome,mx_Activity_Disposition,"
            },
            "Paging": {
                "PageIndex": i,
                "PageSize": 5000
            }

        }
        res = requests.post(url, headers=headers, json=dic)
        x1 = res.json()
        x = x1['Leads']
        if not x:
            print(f"list is  empty{i}")
        else:
            lst = []
            lst1 = []
            lst2 = []
            lst3 = []
            lst4 = []
            lst5 = []
            lst6 = []
            lst7 = []
            lst8 = []
            lst9 = []
            lst10 = []
            lst11 = []

            for i in range(len(x)):
                y = x[i]
                y = y['LeadPropertyList']
                k = y[0]
                k = k['Value']
                lst.append(k)
                l = y[1]
                l = l['Value']
                lst1.append(l)
                m = y[2]
                m = m['Value']
                lst2.append(m)
                n = y[3]
                n = n['Value']
                lst3.append(n)
                o = y[4]
                o = o['Value']
                lst4.append(o)
                p = y[5]
                p = p['Value']
                lst5.append(p)
                q = y[6]
                q = q['Value']
                lst6.append(q)
                r = y[7]
                r = r['Value']
                lst7.append(r)
                s = y[8]
                s = s['Value']
                lst8.append(s)
                t = y[9]
                t = t['Value']
                lst9.append(t)
                u = y[10]
                u = u['Value']
                lst10.append(u)
            LSQ = pd.DataFrame(list(zip(lst, lst1, lst2, lst3, lst4, lst5, lst6, lst7, lst8, lst9, lst10)),
                               columns=['FirstName', 'LastName', 'EmailAddress', 'Mobile Number', 'Created On',
                                        'CP_Code', 'CP_Name', 'Lead Stage', 'Last_Activity_Sub_Outcome',
                                        'Last_Activity_Outcome', 'Activity_Disposition'])
            LSQ['LastName'].fillna('', inplace=True)
            LSQ['Full Name'] = LSQ['FirstName'] + LSQ['LastName']
            LSQ.drop(['FirstName', 'LastName'], inplace=True, axis=1)
            LSQ = LSQ[['Full Name', 'EmailAddress', 'Mobile Number', 'CP_Code', 'CP_Name', 'Lead Stage',
                       'Last_Activity_Sub_Outcome', 'Last_Activity_Outcome', 'Activity_Disposition', 'Created On']]
            Lead = pd.concat([Lead, LSQ])
    Lead['Created On'] = pd.to_datetime(Lead['Created On'], format='%Y/%m/%d %H:%M:%S')
    Lead = Lead[Lead['CP_Code'].isnull() == False]
    lst = ['Activity_Disposition', 'Last_Activity_Outcome', 'Last_Activity_Sub_Outcome']
    for i in lst:
        Lead[i].fillna('Fresh Lead', inplace=True)
    Lead.reset_index(inplace=True)
    Lead.drop('index', inplace=True, axis=1)

    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_file('sample.json', scopes=scopes)
    gc = gspread.authorize(credentials)

    spreadsheet = gc.open_by_key('1ZBW8_G0SIFXfn9jaWbA3bsvkpyv08B1HSAycYTNURW4')
    worksheet = spreadsheet.worksheet("CP Lead <> LSQ")
    CP = pd.DataFrame(worksheet.get_all_records())

    Lead = Lead.merge(CP, on='Mobile Number', how="outer")
    Lead['Full Name_x'] = Lead['Full Name_x'].fillna(Lead['Full Name_y'])
    Lead['EmailAddress_x'] = Lead['EmailAddress_x'].fillna(Lead['EmailAddress_y'])
    Lead['CP_Code_x'] = Lead['CP_Code_x'].fillna(Lead['CP_Code_y'])
    Lead['CP_Name_x'] = Lead['CP_Name_x'].fillna(Lead['CP_Name_y'])
    Lead['Lead Stage_x'] = Lead['Lead Stage_x'].fillna(Lead['Lead Stage_y'])
    Lead['Last_Activity_Sub_Outcome_x'] = Lead['Last_Activity_Sub_Outcome_x'].fillna(
        Lead['Last_Activity_Sub_Outcome_y'])
    Lead['Last_Activity_Outcome_x'] = Lead['Last_Activity_Outcome_x'].fillna(Lead['Last_Activity_Outcome_y'])
    Lead['Activity_Disposition_x'] = Lead['Activity_Disposition_x'].fillna(Lead['Activity_Disposition_y'])
    Lead['Created On_x'] = Lead['Created On_x'].fillna(Lead['Created On_y'])
    Lead.rename(columns={'Full Name_x': 'Full Name', 'EmailAddress_x': 'EmailAddress', 'CP_Code_x': 'CP_Code',
                         'CP_Name_x': 'CP_Name', 'Lead Stage_x': 'Lead Stage',
                         'Last_Activity_Sub_Outcome_x': 'Last_Activity_Sub_Outcome',
                         'Last_Activity_Outcome_x': 'Last_Activity_Outcome',
                         'Activity_Disposition_x': 'Activity_Disposition', 'Created On_x': 'Created On'}, inplace=True)
    Lead.drop(['Full Name_y', 'EmailAddress_y', 'CP_Code_y', 'CP_Name_y', 'Lead Stage_y', 'Last_Activity_Sub_Outcome_y',
               'Last_Activity_Outcome_y', 'Activity_Disposition_y', 'Created On_y'], axis=1, inplace=True)

    Lead.to_csv('Lead.csv', index=False)

    Lead.to_csv('Lead.csv', index=False)
    with open('Lead.csv', 'rb') as file_obj:
        content = file_obj.read()
        gc.import_csv(spreadsheet.id, data=content)

    return redirect("https://docs.google.com/spreadsheets/d/1Bbv1NmnAxXTC8Gm4A10hLcQGtYWsaHYgHmGbPHplbog/edit#gid=0")
if __name__ == "__main__":
    app.run(debug=True)
