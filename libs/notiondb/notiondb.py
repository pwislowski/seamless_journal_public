import requests
import json
import pandas as pd
from datetime import datetime as dt

class DtypeWrapper:
    supported = [
        "date",
        "number",
        "multi_select",
        "select",
        "string",
        "formula",
        "title"
    ]
    
    def __init__(self, obj) -> None:
        self.o = obj

    def res(self):
        obj = self.o
        t = obj["type"]

        if t == "date":
            
            try:
                return obj[t]["start"]
            
            except TypeError:
                return pd.NA


        elif t == "number":
            return obj[t]

        elif t == "multi_select":
            lst = []
            for v in obj[t]:
                lst.append(v["name"])

            return lst

        elif t == "select":
            try:
                return obj[t]["name"] 
            except TypeError:
                return pd.NA

        elif t == "string":
            return obj[t]

        elif t == "formula":
            
            typ = obj[t]["type"]
            return obj[t][typ]    
            
        elif t == "title":
            try:
                return obj[t][0]["text"]["content"]
            except IndexError:
                return "error"
        
        elif t == "checkbox":
            return obj[t]

        else:
            return "unsupported"

class Number:
    def __init__(self, name) -> None:
        self.name = name
        self.struct = {"number" : None}
        
        pass

    def wrapper(self, val):

        val = round(val, 5)
        self.struct['number'] = val

        dic = {
            f'{self.name}' : self.struct
        }

        return dic

class Date:
    def __init__(self, name) -> None:
        self.name = name
        self.struct = {
            "date" : {
                "start" : None 
            }
        }

        pass

    def wrapper(self, date:str, time:str):

        isoformat = f'{date}T{time}'
        self.struct['date']['start'] = isoformat

        dic = {
            f'{self.name}' : self.struct
        }

        return dic

class Title:

    def __init__(self, title) -> None:
        self.title = title
        self.struct = {
            "Name" : {
                'title' : [{'text' : {'content' : None}}]
            }
        }

        pass

    def wrapper(self):
        self.struct['Name']['title'][0]['text']['content'] = self.title
        
        return self.struct

class Select:

    def __init__(self, name) -> None:
        self.name = name
        self.struct = {
            "select" : {
                "name" : None 
                }
        }

        pass

    def wrapper(self, val):

        self.struct['select']['name'] = val

        dic = {
            f'{self.name}' : self.struct
        }

        return dic

class NotionClient:
    """
    Notion Clinet for accessing and manipulating Notion dbases.
    """
    def __init__(self, api, idDb) -> None:
        
        self.api = api
        self.idDb = idDb
        self.parent_db = {"parent" : {'database_id': idDb}} 

        self.header = {
                "Authorization": f"Bearer {api}",
                "Content-Type": "application/json",
                "Notion-Version": "2022-06-28"
        }
    
    def get_data(
        self,
        savefile:bool = False,
        f_name:str = 'data_notiondb.json',
        output:str = 'json'
    ):
        URL = f"https://api.notion.com/v1/databases/{self.idDb}/query"
        _request = requests.post(URL, headers = self.header)
        data = _request.json()

        # check if connection established
        try:
            assert _request.status_code == 200
        
        except AssertionError:
            raise ConnectionError("Failed to connect to database.")

        if output == 'json':

            if savefile:
                with open(f_name, 'w') as file_json:
                    json.dump(
                        data,
                        file_json,
                        indent = 4
                    )
                
                return data

            else:
                return data

        if output == 'pd':
            df = self.__pandas_df(data)

            return df
     
    def __pandas_df(self, json_data):

        _data = json_data
        try:
            first_row = _data["results"][0]["properties"]
    
        except IndexError:
            raise "There is no data."

        cols = list(first_row.keys())
        data = {k : [] for k in cols}

        # check for data types
        dtypes_support = set(DtypeWrapper.supported)
        dtypes = [first_row[r]["type"] for r in first_row.keys()]
        dtypes = set(dtypes)

        if dtypes in dtypes_support:
            diff = dtypes - dtypes_support

            raise TypeError(f"Wrapper does not support data type: {diff}")


        # iterate throgh rows
        rows = _data["results"]
        for r in rows:
            
            # iterate through page
            page = r["properties"]
            for col in page.keys():
                obj = page[col]
                obj = DtypeWrapper(obj)

                target_lst = data[col]
                target_lst.append(obj.res())
        
        return pd.DataFrame(data)
    
    def update_property(self, data_json):
        # under development

        patchURL = "https://api.notion.com/v1/databases/{}".format(self.idDb)

        req = requests.patch(
            url = patchURL,
            headers= self.header,
            json = data_json
        )

        if req.status_code == 200:
            print("Updated")
        else:
            raise ConnectionError(f"Failed to update. Erorr: {req.status_code}")
    
    def create_page(self, dic:dict, raw_dic:dict = None):
        URL = "https://api.notion.com/v1/pages"

        page_properties = {
            'properties' : {**dic}
        }
        
        notion_query = {**self.parent_db, **page_properties}

        req = requests.post(
            url = URL,
            headers= self.header,
            json = notion_query
        )

        if req.status_code == 200:
            notion_pageid = json.loads(req.text)

            if raw_dic is not None: #TODO: add functinality to print entries
                print(
                    """New Journal Entry: <check notion>"""
                )
            
        else:
            raise ConnectionError(f"Failed to create an object. Error {req.status_code}")
    
